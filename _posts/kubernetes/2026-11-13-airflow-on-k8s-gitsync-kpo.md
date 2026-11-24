---
title: "쿠버네티스 위에 Airflow 올리기: Helm·git-sync·KubernetesPodOperator로 배치 실행 구조 바꾸기"
excerpt: "1편에서 만든 RKE2 클러스터 위에 Airflow를 Helm으로 배포하고, git-sync로 DAG을 배포하고, KubernetesPodOperator로 task마다 Pod를 띄우는 구조로 데이터 배치를 옮긴 기록이다. 서버에서 코드를 고쳐 복구하던 운영을 '같은 이미지를 다른 인자로 다시 실행'하는 운영으로 바꾼 과정과, 그 과정에서 만난 에러와 회고를 정리했다."
categories: [kubernetes]
tags: [airflow, kubernetes, helm, git-sync, kubernetespodoperator, data-engineering, 포트폴리오]
date: 2026-11-13 10:00:00 +0900
last_modified_at: 2026-11-13
---

## 이 글을 30초에 파악하기

> **한 줄로** — 서버마다 따로 돌던 데이터 배치를 "이미지 하나 + 실행 인자"로 정리하고, 쿠버네티스 위의 Airflow가 task마다 Pod를 띄워 실행하도록 옮긴 기록이다.

**이 글에서 보여주는 것**

- CeleryExecutor 대신 KubernetesExecutor + KubernetesPodOperator(KPO)를 고른 이유와 그 대가
- Airflow Helm 차트 배포에서 실제로 손댄 values 항목 (executor, 마이그레이션 Job, 원격 로그, 타임존)
- git-sync로 DAG을 배포하고, 실행 코드는 Harbor 이미지로 분리한 구조
- DAG별 Secret 주입, 레지스트리 인증, 노드 지정, 공유 볼륨까지 KPO task 하나를 표준화한 방법
- 출발점이 제각각인 레거시 배치를 컨테이너로 옮긴 표준 절차와 그 과정에서 막힌 지점
- 장애 복구 방식이 "운영 서버에서 코드 수정"에서 "인자 바꿔 재실행"으로 바뀐 과정

**용어 먼저 보기**

|용어|한 줄 설명|이 글에서의 역할|
|---|---|---|
|Airflow|워크플로를 DAG(방향성 비순환 그래프)로 정의하고 스케줄링하는 오케스트레이터|"무엇을 언제" 실행할지 결정|
|KubernetesExecutor|task 하나마다 worker Pod를 새로 띄우는 Airflow executor|2장, 상시 worker 없이 필요할 때만 자원 사용|
|KubernetesPodOperator (KPO)|지정한 컨테이너 이미지로 Pod를 띄워 그 안에서 작업을 실행하는 operator|5장, 실행 코드와 Airflow 환경을 완전히 분리|
|git-sync|Git 저장소를 주기적으로 pull해서 Pod 안의 디렉터리와 맞춰 주는 사이드카|4장, DAG 배포|
|Secret|쿠버네티스에 민감 정보를 저장하는 리소스|5.2절, DAG별 접속 정보 주입|
|catchup|DAG의 시작일 이후 실행되지 않은 구간을 Airflow가 자동으로 채워 실행하는 기능|6.8절, 누락 일자 재수집|
|Pool|여러 task가 공유하는 동시 실행 슬롯|7.2절, 외부 API 동시 요청 제한 대응|

**프로젝트 개요**

|항목|내용|
|---|---|
|기간|2025.01 ~ 2025.08 (8개월, PoC → 구축)|
|역할|설계·구축·운영 주도, 배치 코드 컨테이너화와 DAG 재설계|
|대상|트렌드 키워드 수집, 경쟁사 몰 상품 수집, 상품 리뷰 수집, 상품 이미지 아카이빙 등 사내 데이터 수집 배치|
|스택|Airflow 2.9 (Helm chart), KubernetesExecutor, KubernetesPodOperator, git-sync, Harbor, GitLab CI, NFS(RWX), Prometheus·Grafana|

---

이 시리즈는 사내 데이터 수집 배치를 쿠버네티스 위로 옮긴 프로젝트를 두 편으로 나눠 정리한 기록이다.

- **[1편](/kubernetes/onprem-k8s-platform-rke2-harbor-cicd/)**: 실행 플랫폼. 서버 준비부터 RKE2 클러스터, Helm, Harbor, GitLab CI까지 "컨테이너를 돌릴 땅"을 만든 과정
- **2편 (이 글)**: 그 위에 Airflow를 올리고, 배치를 컨테이너로 정리해 task 단위 Pod로 실행한 과정

1편이 "어디서 돌릴 것인가"였다면, 이 글은 "무엇을 어떻게 돌릴 것인가"다.

---

## 1. 배경: 배치가 어떻게 돌고 있었나

### 1.1 AS-IS

1편에서 본 서버 문제와 별개로, 배치 자체의 구조에도 문제가 있었다.

|구분|AS-IS|문제|
|---|---|---|
|실행 단위|서버에 설치된 Python 가상환경 + 스크립트|환경 재현 불가, 서버를 옮기면 처음부터 다시 설치|
|코드 위치|실행 코드와 추출·적재 SQL이 서버 여러 곳과 DAG 파일 안에 흩어짐|장애가 나면 어디의 무엇을 고쳐야 할지부터 찾아야 함|
|실행 값|수집 날짜 같은 값이 코드·쿼리에 고정|복구하려면 운영 서버에서 코드를 직접 수정|
|실행 순서|작업끼리 의존 관계 없이, 겹치지 않을 시각에 하나씩 띄우는 시간차 스케줄|앞 작업이 일찍 끝나도 다음 작업은 정해진 시각까지 대기|
|DAG 구성|수집 채널마다 DAG·크롤러 파일이 따로|채널이 늘수록 파일과 DAG이 같이 늘어남|

### 1.2 가장 아팠던 문제: 복구가 곧 운영 코드 수정이었다

실제로 있었던 사고 패턴은 이랬다.

1. 쿼리 조건이 "오늘 날짜" 기준으로 코드에 들어가 있었다.
2. 전날 데이터를 다시 수집하려고 운영 서버에서 쿼리의 날짜 값을 직접 바꿔 재실행했다.
3. 원래 값으로 되돌리는 걸 잊었다.
4. 다음 날 정기 배치가 수정된 값으로 돌아 엉뚱한 날짜를 수집했다.

원인은 사람의 실수가 아니라 구조였다. 복구 수단이 "운영 코드 수정"밖에 없었고, 수정 이력도 남지 않았다. 이 프로젝트에서 쿠버네티스보다 먼저 고치고 싶었던 게 이 부분이다.

### 1.3 목표

- 작업 하나 = 이미지 하나. 실행 환경을 서버가 아니라 이미지에 담는다.
- 실행할 때마다 달라지는 값은 코드가 아니라 **실행 인자**로 받는다.
- DAG은 "어떤 이미지를, 어떤 인자로, 어떤 순서로"만 정한다.
- 복구는 코드 수정이 아니라 **같은 이미지를 다른 인자로 다시 실행**하는 것으로 끝나게 한다.

---

## 2. 설계: Airflow는 "언제", 쿠버네티스는 "어디서"

### 2.1 Executor 선택

|선택지|동작|판단|
|---|---|---|
|CeleryExecutor|상시 떠 있는 worker들이 큐에서 task를 가져가 실행|기존 방식. 서버 한 대에서 worker를 늘리는 건 1편의 실측(작업 두 개에 CPU 포화)으로 한계 확인|
|KubernetesExecutor|task마다 worker Pod를 새로 띄우고 끝나면 삭제|상시 worker가 없어 유휴 자원이 없음. 클러스터 전체 노드에 분산|
|KubernetesExecutor + **KPO**|worker Pod가 다시 task 전용 이미지로 Pod를 띄움|**선택**. task마다 이미지·리소스·Secret을 따로 줄 수 있어 실행 코드가 Airflow 환경에 묶이지 않음|

KPO를 고른 결정적인 이유는 **의존성 분리**였다. PythonOperator로 돌리면 모든 작업의 라이브러리가 Airflow 이미지 하나에 섞인다. KPO로 돌리면 크롤러는 크롤러 이미지, 적재 작업은 적재 이미지를 쓰고, Airflow 이미지는 오케스트레이션만 담당한다.

**대가도 있었다.** task마다 Pod를 띄우니 기동 지연이 생긴다. 같은 재설계를 기존 방식으로 돌렸을 때보다 KPO 구조가 몇 분 더 걸렸다. 그 몇 분을 내고 task 격리, 환경 재현, 배포 표준을 얻은 선택이었다.

### 2.2 전체 구조

```
[개발자]
  │ git push (실행 코드 + Dockerfile)          git push (DAG)
  ▼                                             ▼
GitLab ──▶ GitLab Runner (빌드·push)          GitLab (DAG 저장소)
             │                                  │ git-sync
             ▼                                  ▼
          Harbor (이미지 레지스트리)        Airflow (KubernetesExecutor)
             │ imagePull                        │ "무엇을 언제"
             └──────────────▶ KPO Pod (airflow-task 네임스페이스)
                                │ "어디서 어떻게" = Kubernetes Scheduler
                                ├─ env: DAG별 Secret (DB 접속 정보, API Key)
                                ├─ args: --run-date, --channel ...
                                ├─ volume: NFS RWX PVC (task 간 공유 데이터)
                                └─ 결과: S3 / DB 적재
```

위 다이어그램은 코드가 두 갈래로 배포된다는 점을 보여준다. 실행 코드는 CI를 거쳐 **이미지**로, DAG은 git-sync를 거쳐 **파일**로 들어간다. 그래서 DAG을 고쳐도 실행 코드는 그대로고, 실행 코드를 고쳐도 DAG은 이미지 태그만 바꾸면 된다. 변경 영향 범위가 둘로 나뉜다.

### 2.3 네임스페이스 분리

|네임스페이스|들어가는 것|이유|
|---|---|---|
|`airflow`|scheduler, webserver, triggerer, 메타DB 연결|Airflow 자체 컴포넌트|
|`airflow-task`|KPO가 띄우는 작업 Pod, 작업용 Secret, 레지스트리 인증 Secret|작업 Pod의 권한·Secret을 Airflow 본체와 분리|

---

## 3. Airflow Helm 배포

### 3.1 설치

공식 Apache Airflow Helm 차트를 썼다. Rancher의 Apps 화면에서 설치할 수도 있지만, values를 파일로 남기려고 CLI로 설치했다.

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow --create-namespace \
  -f values-airflow.yaml \
  --timeout 10m0s
```

### 3.2 실제로 손댄 values

기본값에서 바꾼 항목만 추리면 이렇다.

```yaml
executor: KubernetesExecutor

# 웹서버 세션 서명 키. 지정하지 않으면 재배포 때마다 바뀌어 로그인이 풀린다
webserverSecretKey: <RANDOM_HEX>   # python3 -c 'import secrets; print(secrets.token_hex(16))'

# Rancher/helm --wait 환경에서 hook Job과 Pod 기동이 서로 기다리는 문제를 피하려고 hook 비활성화
migrateDatabaseJob:
  useHelmHooks: false
  applyCustomEnv: false
createUserJob:
  useHelmHooks: false
  applyCustomEnv: false

# task Pod는 끝나면 사라지므로 로그를 밖에 남긴다
config:
  logging:
    remote_logging: "True"
    remote_base_log_folder: s3://<LOG_BUCKET>/airflow_logs
    remote_log_conn_id: aws_default
    delete_local_logs: "True"

# 타임존 통일 (Rancher는 KST, Airflow는 UTC로 찍혀 로그 대조가 어려웠다)
env:
  - name: TZ
    value: "Asia/Seoul"
  - name: AIRFLOW__CORE__DEFAULT_TIMEZONE
    value: "Asia/Seoul"
  - name: AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE
    value: "Asia/Seoul"
```

**원격 로그가 필수인 이유**: KubernetesExecutor에서는 task가 끝나면 worker Pod가 삭제된다. 로그를 Pod 안에만 두면 실패한 task의 로그를 UI에서 볼 수 없다. 처음 PoC에서 바로 겪었고, S3 원격 로그를 켜서 해결했다.

### 3.3 설치하며 만난 에러

|증상|원인|해결|
|---|---|---|
|scheduler·webserver가 `wait-for-airflow-migrations`에서 멈춤|helm hook으로 도는 마이그레이션 Job이 `--wait` 완료 뒤에야 실행돼 서로 기다림|`migrateDatabaseJob.useHelmHooks: false`|
|PVC가 Pending에서 안 넘어감|기본 StorageClass가 지정되지 않음|StorageClass를 default로 지정 (values에서 `storageClassName`을 비워 두면 기본 클래스 사용)|
|재배포할 때마다 웹 UI 로그인이 풀림|webserver secret key가 매번 새로 생성됨|`webserverSecretKey` 고정|
|UI 시각과 Rancher 로그 시각이 9시간 차이|컴포넌트마다 타임존이 다름|전역 `env`로 TZ와 Airflow 기본 타임존을 KST로 통일|

---

## 4. git-sync로 DAG 배포

### 4.1 왜 git-sync인가

DAG을 배포하는 방법은 크게 세 가지다.

|방식|장점|단점|
|---|---|---|
|DAG을 Airflow 이미지에 굽기|버전이 이미지에 고정|DAG 한 줄 고칠 때마다 Airflow 이미지 재빌드·재배포|
|PVC에 DAG 파일 복사|단순|누가 언제 무엇을 바꿨는지 이력이 없음|
|**git-sync 사이드카**|Git에 merge하면 자동 반영, 이력은 Git에 남음|저장소 인증 설정이 필요|

DAG은 자주 바뀌고, 이력이 남아야 했다. git-sync를 택했다.

### 4.2 설정

```yaml
dags:
  persistence:
    enabled: false
  gitSync:
    enabled: true
    repo: https://gitlab.example.internal/data/dags.git
    branch: main
    subPath: "dags"
    credentialsSecret: git-credentials
```

인증 정보는 Secret으로 넣는다. 차트 버전에 따라 git-sync v3/v4가 읽는 키 이름이 달라서, 두 쌍을 모두 넣어 두는 편이 안전했다.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: git-credentials
  namespace: airflow
type: Opaque
stringData:
  GIT_SYNC_USERNAME: <USERNAME>
  GIT_SYNC_PASSWORD: <ACCESS_TOKEN>
  GITSYNC_USERNAME: <USERNAME>
  GITSYNC_PASSWORD: <ACCESS_TOKEN>
```

### 4.3 git-sync에서 만난 에러

|증상|원인|해결|
|---|---|---|
|`git-sync-init` 컨테이너가 `HTTP Basic: Access denied`로 반복 실패|2FA가 켜진 GitLab 저장소는 비밀번호 인증이 안 됨|비밀번호 대신 Access Token 사용 (가능하면 읽기 전용 Project Access Token)|
|Secret을 넣었는데도 인증 실패|차트가 기대하는 키 이름과 Secret의 키 이름이 다름|v3·v4 키를 모두 등록|
|DAG이 UI에 안 보임|`subPath`가 저장소의 실제 DAG 경로와 다름|`kubectl logs <scheduler-pod> -c git-sync`로 동기화 경로 확인 후 수정|

디버깅은 거의 항상 `kubectl logs <pod> -c git-sync-init`에서 시작했다. 사이드카 로그에 git 명령과 실패 사유가 그대로 찍힌다.

---

## 5. KubernetesPodOperator: task 하나를 표준화하기

### 5.1 기본 형태

모든 작업 task는 아래 모양으로 통일했다.

```python
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

collect = KubernetesPodOperator(
    task_id="collect_reviews",
    name="collect-reviews",
    namespace="airflow-task",
    image="harbor.example.internal/data/review-collector:1.4.2",
    image_pull_secrets=[k8s.V1LocalObjectReference("harbor-auth")],
    arguments=["--run-date", "{% raw %}{{ ds }}{% endraw %}"],          # 실행 값은 인자로
    env_from=[k8s.V1EnvFromSource(                  # 접속 정보는 Secret으로
        secret_ref=k8s.V1SecretEnvSource(name="review-secret"))],
    container_resources=k8s.V1ResourceRequirements(
        requests={"cpu": "500m", "memory": "1Gi"},
        limits={"memory": "2Gi"}),
    on_finish_action="delete_pod",
    get_logs=True,
)
```

여기서 정한 원칙은 세 가지다.

- **이미지 태그를 고정한다.** `latest`를 쓰면 언제 어떤 코드가 돌았는지 추적할 수 없다.
- **실행 값은 `arguments`로 넘긴다.** `{% raw %}{{ ds }}{% endraw %}`처럼 Airflow가 실행 구간의 날짜를 넣어 준다.
- **민감 정보는 `env_from` Secret으로만 넣는다.** 이미지와 DAG 어디에도 접속 정보가 없다.

### 5.2 Secret은 왜 DAG별로 넣었나

|방식|동작|판단|
|---|---|---|
|Helm values의 `podTemplate.envFrom`|모든 task Pod에 같은 Secret이 들어감|DAG마다 접속하는 DB가 달라 맞지 않음|
|**KPO의 `env_from`**|task마다 다른 Secret 지정|**선택**. DAG별로 필요한 접속 정보만 주입|

Secret은 Rancher UI에서 만들고 고칠 수 있게 했다. 팀원이 kubectl 없이도 접속 정보를 바꿀 수 있다.

### 5.3 레지스트리 인증은 작업 네임스페이스에

1편에서 Harbor robot 계정으로 만든 `harbor-auth` Secret은 **KPO Pod가 뜨는 `airflow-task` 네임스페이스에** 있어야 한다. `airflow` 네임스페이스에만 만들어 두고 `ImagePullBackOff`를 본 적이 있다. Secret은 네임스페이스 단위라는 걸 몸으로 배웠다.

### 5.4 노드 지정은 예외로만

원칙은 스케줄러에 맡기는 것이다. 다만 노드마다 네트워크 조건이 달랐다.

- 외부 사이트 수집은 프록시가 있는 노드에서만 성공했다.
- 특정 외부 DB는 접근이 허용된 노드에서만 붙었다.

이런 작업만 노드에 라벨을 달고 `node_selector`로 고정했다.

```python
KubernetesPodOperator(
    ...,
    node_selector={"egress": "proxy"},
)
```

나머지 일반 task는 처음에 붙여 뒀던 node_selector를 운영 중에 걷어내 자동 분산으로 돌렸다. 쿠버네티스 원칙과는 어긋나는 타협이라, 예외 목록을 짧게 유지하는 게 중요했다.

### 5.5 task 사이에 파일을 넘겨야 할 때

작업끼리 중간 파일을 주고받아야 하는 경우가 있다. Pod는 task마다 새로 뜨고 어느 노드에서 뜰지 모르니, **어느 노드에서든 같은 경로가 보이는 RWX 볼륨**이 필요하다.

```python
shared = k8s.V1Volume(
    name="shared",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="airflow-shared"))
mount = k8s.V1VolumeMount(name="shared", mount_path="/data")

KubernetesPodOperator(..., volumes=[shared], volume_mounts=[mount])
```

처음에는 local-path PVC를 썼는데, PVC가 처음 바인딩된 노드에 고정돼서 해당 task가 그 노드에서만 뜨는 문제가 생겼다. 워커를 늘려도 의미가 없어서 공통 저장소 서버의 NFS(RWX)로 옮겼고, 보존 기간 30일을 `systemd-tmpfiles`로 걸었다.

### 5.6 디버깅용 테스트 Pod

KPO가 실패하면 Pod가 바로 지워져 안을 들여다보기 어렵다. 같은 이미지·Secret·볼륨으로 잠들어 있는 Pod를 띄워 두고 들어가서 확인하는 템플릿을 만들어 뒀다.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: debug-review-collector
  namespace: airflow-task
spec:
  containers:
    - name: app
      image: harbor.example.internal/data/review-collector:1.4.2
      command: ["/bin/sh", "-c", "sleep 3600"]
      envFrom:
        - secretRef:
            name: review-secret
  imagePullSecrets:
    - name: harbor-auth
```

```bash
kubectl apply -f debug-pod.yaml
kubectl exec -it debug-review-collector -n airflow-task -- /bin/sh
```

"Airflow 문제인지, 이미지 문제인지, 네트워크 문제인지"를 가장 빨리 가르는 방법이었다.

---

## 6. 레거시 코드 컨테이너화: 코드에서 서버와 실행 값을 빼다

이 프로젝트에서 운영 측면으로 가장 크게 바뀐 부분이자, 시간이 가장 오래 걸린 부분이다. 8개월 중 클러스터와 에코시스템 구축보다 흩어진 레거시 코드를 하나씩 이미지로 옮기는 데 훨씬 많은 시간을 썼다.

### 6.1 왜 오래 걸렸나: 출발점이 제각각이었다

옮겨야 할 배치는 여러 사람이 여러 시기에 만든 코드였다. 같은 "배치"라도 출발점이 달라서 하나의 방법으로 일괄 전환할 수 없었다.

|출발점 유형|모습|컨테이너로 옮길 때 걸린 점|
|---|---|---|
|서버 스크립트형|서버의 Python 가상환경 + 스크립트를 스케줄러가 호스트 경로로 호출|서버 경로, 로컬 파일, 서버에 깔린 패키지에 암묵적으로 의존|
|DAG 내장형|추출·적재 로직이 DAG 파일 안의 PythonOperator 함수로 들어가 있음|DAG 파일이 사실상 코드 저장소. 모든 작업의 라이브러리가 Airflow 환경에 섞임|
|채널 복제형|수집 채널마다 크롤러 파일과 DAG이 하나씩, 키워드 풀만 다른 같은 코드가 DAG 두 개로 운영|파일 수만큼 옮길 대상이 늘어남. 어느 파일이 진짜 운영 중인지부터 확인해야 함|
|노트북형|PC에서 Jupyter 셀을 순서대로 수동 실행, 결과도 수동 업로드|실행 순서가 사람 머릿속에 있음. 스크립트로 만드는 것부터 시작|

게다가 전부 **지금 돌고 있는 운영 배치**였다. 멈추고 옮길 수 없으니, 기존 배치를 그대로 두고 새 이미지를 옆에서 돌려 결과를 대조한 뒤 하나씩 넘겼다. 시간이 오래 걸린 건 기술이 어려워서라기보다 이 "확인하고 넘기기"를 작업마다 반복해야 했기 때문이다.

### 6.2 표준 전환 절차

몇 개를 옮겨 보고 나서 순서를 고정했다. 원칙은 **로직은 최대한 그대로 두고 경계만 바꾼다**였다. 리팩터링 욕심을 내면 끝이 없어서, 수집·계산 로직은 손대지 않고 "어디서 값을 받고, 어디에 결과를 쓰고, 어떻게 실행되는가"만 바꿨다.

1. **운용 목록 확정**: 실제 스케줄에 걸려 있는 파일만 남긴다. 일회성 마이그레이션·복구 스크립트, 노트북, CSV·zip, 캐시는 저장소에서 뺀다.
2. **설정·상수 분리**: 코드에 박힌 버킷 이름, 프록시 주소, 접속 정보를 설정 파일과 환경 변수로 뺀다.
3. **함수화·모듈화**: 날짜 처리, 파일 입출력, S3 연결처럼 스크립트마다 반복되던 코드를 공통 함수로 모은다.
4. **SQL 분리**: 코드 문자열로 섞여 있던 쿼리를 yaml 파일로 뺀다.
5. **진입점 통일 + 실행 인자**: 실행 파일을 진입점 하나로 모으고, 날짜·채널 같은 값은 인자로 받는다.
6. **이미지화 + 로컬 검증**: Dockerfile을 쓰고, `docker compose`로 운영과 같은 인자로 돌려 기존 배치와 결과를 대조한다.
7. **CI·Secret·DAG 연결**: 1편의 온보딩 4단계(이미지 CI → Harbor → DAG 추가 → Secret 구성)로 클러스터에 올린다.

1~5단계가 코드 정리, 6~7단계가 배포다. 시간은 대부분 1~5단계에서 들었다.

### 6.3 이미지 하나, 진입점 하나

정리가 끝난 작업 저장소는 이런 모양이 된다.

```
review-collector/
├── Dockerfile
├── requirements.txt
├── .gitlab-ci.yml
├── queries/
│   └── review.yaml       # SQL은 코드 밖으로
└── collector/
    ├── main.py           # 진입점: 인자 파싱 → 작업 선택
    ├── config.py         # 상수·환경 변수 읽기
    ├── extract.py
    ├── load.py
    └── utils.py          # 날짜·S3·파일 공통 함수
```

```yaml
# queries/review.yaml
extract_reviews: |
  SELECT product_id, review_text, created_at
  FROM reviews
  WHERE created_at >= %(run_date)s
    AND created_at <  %(run_date)s::date + 1
```

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENTRYPOINT ["python", "-m", "collector.main"]
```

쿼리에서 날짜가 `%(run_date)s` 파라미터로 바뀐 것이 중요하다. 1.2절의 사고는 이 자리에 날짜가 숫자로 박혀 있어서 생겼다.

### 6.4 실행 값은 인자로, 채널은 인자 하나로

```python
# collector/main.py
import argparse
from datetime import date, timedelta
from collector.crawlers import MallACrawler, MallBCrawler

CRAWLERS = {"mall_a": MallACrawler, "mall_b": MallBCrawler}

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--run-date", type=date.fromisoformat,
                   default=date.today() - timedelta(days=1))
    p.add_argument("--channel", required=True, choices=CRAWLERS)
    return p.parse_args()

def main():
    args = parse_args()
    CRAWLERS[args.channel](run_date=args.run_date).run()
```

채널마다 따로 있던 크롤러 파일은 공통 인터페이스를 가진 클래스로 정리하고, 채널 이름으로 구현을 고르게 했다(팩토리 패턴). 새 채널을 붙일 때 파일과 DAG을 만드는 대신 클래스 하나를 추가하고 인자 목록에 이름 하나를 넣으면 된다.

같은 코드를 키워드 풀만 바꿔 DAG 두 개로 돌리던 작업도 같은 방식으로 합쳤다. 키워드 풀은 설정으로 빼고, 어떤 풀을 쓸지는 `--keyword-set` 인자 하나로 정한다.

### 6.5 DAG 안의 코드를 이미지로

DAG 내장형은 이렇게 바뀌었다.

**전**: DAG 파일이 코드를 품고 있다.

```python
def extract_and_load(**context):
    conn = connect(host="...", password="...")          # 접속 정보가 DAG에
    rows = conn.execute("SELECT ... WHERE dt = '2025-03-02'")  # 날짜가 코드에
    ...

PythonOperator(task_id="extract_load", python_callable=extract_and_load)
```

**후**: DAG은 "어떤 이미지를 어떤 인자로"만 말한다.

```python
KubernetesPodOperator(
    task_id="extract_load",
    image="harbor.example.internal/data/review-collector:1.4.2",
    arguments=["extract", "--run-date", "{% raw %}{{ ds }}{% endraw %}"],
    env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="review-secret"))],
    ...
)
```

추출, 적재, SQL 실행처럼 PythonOperator가 하던 일은 이미지 안의 실행 파일로 옮겼다. 그 결과 DAG 저장소에는 오케스트레이션만 남았고, 로직을 고치면 반드시 CI를 거쳐 새 이미지 태그가 생긴다.

### 6.6 로컬 디스크에 기대지 않기

서버에서 돌 때는 "일단 로컬 디스크에 저장하고 나중에 올리기"가 자연스러웠다. 컨테이너는 끝나면 디스크가 사라지고, 다음 task는 다른 노드에서 뜰 수도 있다.

|기존|바꾼 방식|
|---|---|
|로컬 디스크에 저장한 뒤 S3·DB로 옮김|처음부터 S3·DB로 바로 적재|
|중간 계산 결과를 DB에 쓰고 다시 읽음|파일(parquet)로 저장해 DB I/O를 줄임|
|다음 task가 같은 서버의 파일을 읽음|task 사이 공유 파일은 NFS RWX 볼륨(5.5절)|

이 변경은 컨테이너화를 위해 한 것이지만, 저장 단계가 하나 줄면서 실행 시간과 실패 지점도 함께 줄었다.

### 6.7 옮기면서 막힌 것

|증상|원인|해결|
|---|---|---|
|로컬에서는 되는 외부 수집이 Pod에서는 실패|노드마다 egress(프록시) 조건이 다름|프록시를 쓸 수 있는 노드를 붙이고 `node_selector`로 지정(5.4절). 그 전까지는 일시적으로 기존 방식 유지|
|Pod 안에서 원인을 찾기 어려움|가벼운 이미지라 편집기·도구가 없고, task Pod는 끝나면 사라짐|디버그용 테스트 Pod(5.6절), 클러스터 위에 JupyterHub를 올려 같은 네트워크 조건의 테스트 환경 마련|
|컨테이너 안에서 사내 호스트 이름을 못 찾음|컨테이너에서는 `/etc/hosts`를 직접 고칠 권한이 없음|Pod 수준 DNS 설정(`hostAliases`)으로 해결, 이후 클러스터 DNS 구조 정비|
|이미지가 커서 노드 디스크 압박, Evicted|무거운 베이스 이미지, 불필요한 파일 포함|slim 베이스, 운용 목록 밖 파일 제거(6.2절 1단계)|
|새 이미지 결과가 기존 배치와 미묘하게 다름|호출 방식·계산 방식 변경의 부작용|합계와 추세를 기존 결과와 대조하고, 허용 범위를 현업과 합의한 뒤 전환|

마지막 줄이 특히 중요했다. 같은 데이터를 다른 방식으로 계산하면 숫자가 조금씩 달라진다. 수치가 달라도 증감 추세가 같은지 확인하고, 데이터를 쓰는 쪽과 합의한 다음에 넘겼다.

### 6.8 catchup은 날짜를 인자로 받는 DAG에만

|DAG 성격|catchup|이유|
|---|---|---|
|`--run-date`를 받아 해당 날짜를 수집|**켬**|빠진 날짜를 Airflow가 구간별로 다시 실행해 준다|
|DB에서 "아직 처리 안 된 날짜"를 직접 읽어 처리|끔|catchup까지 켜면 같은 날짜를 중복 수집|

### 6.9 복구 방식이 바뀌었다

|구분|전|후|
|---|---|---|
|어제 데이터 재수집|운영 서버에서 쿼리 날짜를 직접 수정 → 실행 → (잊지 말고) 되돌리기|Airflow에서 해당 날짜 구간을 clear 하거나 `--run-date`만 바꿔 한 번 더 실행|
|수정 지점|서버 여러 곳 + DAG 파일|프로젝트 저장소 한 곳|
|코드 변경 이력|없음|모든 변경이 CI를 거쳐 이미지 태그로 남음|
|"되돌리기" 단계|있음 (그래서 사고가 남)|없음. 정기 배치는 이미지 기본 동작 그대로|

이 구조의 효과를 운영에서 직접 확인한 일이 있다. 클러스터 밖에 남아 있던 레거시 Airflow 서버가 메타DB 장애로 복구가 불가능해졌을 때, 그 서버의 작업을 위 절차로 컨테이너화해 클러스터로 옮기는 것으로 복구했다. 컨테이너로 옮긴 작업은 서버가 사라져도 이미지와 DAG만 있으면 다시 돌릴 수 있었다.
---

## 7. DAG 재설계: 운영이 편해진 부분

DAG도 다시 짰다. 다만 여기서 쓴 건 TaskGroup, Dynamic Task Mapping, Pool 같은 Airflow의 기본 기능이고, 특별한 기법이라기보다 **원래 있던 기능을 제대로 쓴 것**에 가깝다. 그래서 이 장은 "무엇을 썼나"보다 "운영에서 무엇이 나아졌나"로 정리한다.

### 7.1 시간차 스케줄 → 의존성 기반 실행

이전에는 작업 A를 3시에, B를 4시에, C를 9시에 띄우는 식이었다. A가 10분 만에 끝나도 B는 4시까지 기다렸고, A가 늦어지면 B는 덜 준비된 데이터로 돌았다.

```python
with DAG("weekly_trend", schedule="0 3 * * MON", catchup=False) as dag:
    with TaskGroup("keywords") as keywords:
        ...
    with TaskGroup("rankings") as rankings:
        ...
    report = KubernetesPodOperator(task_id="build_report", ...)

    [keywords, rankings] >> report
```

채널별 수집처럼 같은 작업을 인자만 바꿔 여러 번 돌리는 경우는 Dynamic Task Mapping으로 한 task 정의에서 펼쳤다.

```python
KubernetesPodOperator.partial(
    task_id="collect", image="harbor.example.internal/data/mall-collector:2.0.1", ...
).expand(arguments=[["--channel", c, "--run-date", "{% raw %}{{ ds }}{% endraw %}"] for c in CHANNELS])
```

**운영에서 나아진 점**

- **대기 시간이 사라졌다.** 앞 작업이 끝나면 바로 다음이 시작된다. 주간 배치 하나는 전체 소요 시간이 몇 시간에서 1시간 이내로 줄었는데, 이건 실행이 빨라진 게 아니라 기다리던 시간이 없어진 결과다.
- **실패가 아래로 번지지 않는다.** 앞 작업이 실패하면 하위 작업이 멈추니, 덜 준비된 데이터로 리포트가 만들어지는 일이 없다.
- **재실행 단위가 명확해졌다.** 실패한 TaskGroup이나 mapped task 하나만 clear 하면 된다.
- **관리할 DAG 수가 줄었다.** 채널·주기별로 흩어져 있던 DAG 26개를 업무 단위 4개로 묶었다. 채널을 추가해도 DAG은 늘지 않는다.

### 7.2 병렬로 돌릴 것과 순차로 돌릴 것

클러스터로 오니 병렬로 돌릴 자원이 생겼다. 그런데 무조건 병렬로 돌리면 외부 API의 동시 요청 제한에 걸린다. 실제로 검색 API를 치는 task들이 동시에 돌면서 수집 에러가 났다.

DAG 구조는 그대로 두고, 같은 API를 치는 task만 슬롯 1개짜리 Pool에 넣어 직렬화했다.

```bash
airflow pools set search_api 1 "external search API rate limit"
```

```python
KubernetesPodOperator(task_id="search_volume", pool="search_api", ...)
```

|구분|실행 방식|기준|
|---|---|---|
|서로 독립인 채널·키워드 그룹|병렬 (TaskGroup끼리, mapped task끼리)|클러스터 자원이 받쳐 줌|
|같은 외부 API·같은 소스 DB를 치는 작업|Pool로 직렬|외부 제한이 병렬의 상한|

API 오류에는 task 단위 재시도(1분 간격 3회)를 걸어 일시적 실패가 배치 전체 실패로 번지지 않게 했다.

---

## 8. 운영하며 만난 문제

|시점|증상|원인|조치|재발 방지|
|---|---|---|---|---|
|PoC|KPO Pod가 한꺼번에 떠서 Evicted, Rancher까지 접속 불가|이미지가 크고 루트 파티션이 작음|파티션 확장|이미지 경량화, 디스크 알림|
|구축 초기|`ImagePullBackOff`|`harbor-auth`가 작업 네임스페이스에 없음|`airflow-task`에 Secret 생성|온보딩 절차에 포함|
|워커 추가 후|특정 task가 한 노드에서만 뜸|local-path PVC가 노드에 고정|NFS RWX로 전환|PV 작성 규칙 문서화|
|운영 1년 차|Scheduler 중단, 간헐적 DNS timeout|노드 인증서 만료 + 서로 통신이 안 되는 노드 사이로 DNS 질의가 분산|인증서 갱신, CoreDNS를 한곳에 모으고 노드마다 NodeLocal DNSCache|인증서 만료 알림, 정기 재시작|
|NFS 전환 후|전 노드에서 `volume node affinity conflict`|NFS PV에 로컬 PV 템플릿의 빈 nodeAffinity가 남음|PV·PVC 재생성|NFS PV는 nodeAffinity 없이 작성|
|NFS 전환 후|일부 노드에서 mount timeout / access denied|방화벽(2049) 차단 / exports에 노드 누락|방화벽 오픈, exports 추가|timeout은 포트, denied는 권한으로 구분|

Airflow 레벨에서는 REST API로 실패 task를 모아 Slack으로 보내는 알림 DAG과 일별 요약 DAG을 두었다. 인프라 쪽 알림(디스크, CPU·메모리 10분 지속, 인증서 만료, 헬스체크)은 Grafana Alerting으로 Slack에 보냈다. "현업이 먼저 장애를 발견하는" 운영에서 "시스템이 먼저 알려 주는" 운영으로 바뀐 게 체감상 가장 컸다.

---

## 9. 회고: 다시 한다면

|한계|왜 그렇게 됐나|다시 한다면|
|---|---|---|
|Helm values 일부를 UI·patch로 반영|빠른 구축 우선|values와 매니페스트를 Git으로 관리하고, 규모가 커지면 ArgoCD 검토|
|KPO 기동 지연|task마다 Pod 생성|짧은 task가 많은 DAG은 묶어서 Pod 수를 줄이고, 이미지를 가볍게|
|노드 지정 예외가 남음|노드마다 방화벽·프록시 조건이 다름|노드 라벨·taint를 표준화하고 egress 경로를 하나로|
|NFS 전환 때 일부 데이터 유실|볼륨 데이터의 성격(임시/영구)을 분류하기 전에 전환|데이터 분류 → 백업 → 전환 순서|
|버전 노후 (Airflow 2.9)|운영 안정 우선|정기 업그레이드 주기를 운영 일정에 포함|

---

## 10. 실무 체크리스트

- [ ] task가 끝나면 Pod가 사라지는 구조라면 원격 로그가 켜져 있는가.
- [ ] `webserverSecretKey`를 고정했는가.
- [ ] git-sync 인증에 비밀번호가 아니라 읽기 전용 토큰을 쓰는가.
- [ ] KPO 이미지 태그가 `latest`가 아니라 고정 버전인가.
- [ ] 컨테이너로 옮기기 전에 실제 운용 중인 파일 목록을 확정했는가.
- [ ] 새 이미지의 결과를 기존 배치와 대조하고, 차이의 허용 범위를 데이터 사용자와 합의했는가.
- [ ] 실행할 때마다 달라지는 값(날짜, 채널)이 코드가 아니라 인자로 들어오는가.
- [ ] 접속 정보가 이미지·DAG이 아니라 Secret에만 있는가.
- [ ] 레지스트리 인증 Secret이 작업 Pod가 뜨는 네임스페이스에 있는가.
- [ ] catchup은 날짜를 인자로 받는 DAG에만 켜져 있는가.
- [ ] 외부 API 제한이 있는 task는 Pool로 동시성을 제한했는가.
- [ ] task 사이 공유 파일은 RWX 볼륨에 있고, 보존 기간이 정해져 있는가.
- [ ] 실패 알림이 사람이 보는 채널(Slack 등)로 가는가.

---

## 11. 정리

- **실행 환경을 서버에서 이미지로 옮겼다.** 작업 하나 = 이미지 하나 + 진입점 하나로, 서버가 사라져도 다시 돌릴 수 있게 했다.
- **실행 값을 코드에서 뺐다.** 날짜와 채널을 인자로 받게 바꾸자 복구가 "운영 코드 수정"에서 "다른 인자로 재실행"이 됐다.
- **Airflow와 쿠버네티스의 역할을 나눴다.** Airflow는 무엇을 언제, 쿠버네티스는 어디서 어떻게. KPO로 그 경계를 지켰다.
- **DAG은 원래 있는 기능을 제대로 썼다.** 의존성 기반 실행, TaskGroup, Dynamic Task Mapping, Pool로 대기 시간과 DAG 수가 줄고 재실행이 쉬워졌다.
- **편해진 만큼 대가도 기록했다.** Pod 기동 지연, 노드 지정 예외, 공유 스토리지 단일 장애점은 회고 표에 남겼다.

[1편](/kubernetes/onprem-k8s-platform-rke2-harbor-cicd/)의 클러스터가 "돌릴 땅"이었다면, 이 글의 작업은 그 위에서 배치가 **재현 가능하고 복구하기 쉬운 형태**로 돌게 만든 것이었다.

---

## References

설정 항목은 각 도구의 공식 문서를 기준으로 했고, 에러 사례와 판단 근거는 프로젝트 진행 중 직접 기록한 작업 일지를 바탕으로 했다. 사내 호스트명·주소·버킷·서비스 이름은 예시 값으로 바꿨다.

**Airflow**

- Apache Airflow — [Helm Chart](https://airflow.apache.org/docs/helm-chart/stable/index.html)
- Apache Airflow — [Helm Chart Production Guide (webserver secret key)](https://airflow.apache.org/docs/helm-chart/stable/production-guide.html)
- Apache Airflow — [Manage DAG files (git-sync)](https://airflow.apache.org/docs/helm-chart/stable/manage-dag-files.html)
- Apache Airflow — [Kubernetes Executor](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/kubernetes_executor.html)
- Apache Airflow — [KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html)
- Apache Airflow — [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)
- Apache Airflow — [Pools](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html)
- Apache Airflow — [DAG Runs (catchup)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html)

**쿠버네티스**

- Kubernetes — [Secrets](https://kubernetes.io/docs/concepts/configuration/secret/)
- Kubernetes — [Persistent Volumes (access modes)](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
- Kubernetes — [NodeLocal DNSCache](https://kubernetes.io/docs/tasks/administer-cluster/nodelocaldns/)
- git-sync — [kubernetes/git-sync](https://github.com/kubernetes/git-sync)
