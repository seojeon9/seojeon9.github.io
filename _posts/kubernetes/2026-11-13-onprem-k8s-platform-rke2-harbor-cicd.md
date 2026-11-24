---
title: "사내 서버 4대로 쿠버네티스 플랫폼 만들기: RKE2·Helm·Harbor·GitLab CI"
excerpt: "흩어진 배치 서버 3대를 하나의 RKE2 클러스터로 묶고, Rancher·Helm으로 플랫폼 컴포넌트를 올리고, Harbor와 GitLab Runner로 이미지 빌드·배포 표준을 만든 과정을 정리했다. 폐쇄망·사설 인증서·구형 서버라는 제약 속에서 내린 선택과 트러블슈팅, 그리고 1년 넘게 운영하며 얻은 회고를 담았다."
categories: [kubernetes]
tags: [kubernetes, rke2, rancher, helm, harbor, gitlab-ci, on-premise, 포트폴리오]
date: 2026-11-13 09:00:00 +0900
last_modified_at: 2026-11-13
---

## 이 글을 30초에 파악하기

> **한 줄로** — 서버마다 따로 돌던 데이터 배치를 컨테이너로 돌릴 수 있도록, 사내 서버 4대로 쿠버네티스 클러스터와 이미지 빌드·저장 체계를 직접 구축한 기록이다.

**이 글에서 보여주는 것**

- 서버 단위 운영의 한계를 실측으로 확인하고, 클러스터로 가기로 결정한 근거
- 서버 4대의 역할 분리(컨트롤 플레인 / 워커 / 공통 저장소)와 RKE2·Rancher·Helm 설치 순서
- 폐쇄망에서 Harbor 사설 레지스트리와 GitLab Runner(DinD)로 "코드 push → 이미지 빌드 → 레지스트리 저장" 파이프라인을 만든 과정과 그 과정에서 만난 에러들
- 1년 넘게 운영하면서 다시 한다면 바꿀 것들

**용어 먼저 보기**

|용어|한 줄 설명|이 글에서의 역할|
|---|---|---|
|RKE2|Rancher가 만든 쿠버네티스 배포판. containerd 기반, 보안 기본값 강화|2장, 클러스터 본체|
|Rancher|쿠버네티스 클러스터를 웹 UI로 관리하는 도구|2.4절, 팀원이 kubectl 없이 Secret·워크로드 관리|
|Helm|쿠버네티스 앱을 차트(템플릿 + values) 단위로 설치·업그레이드하는 패키지 매니저|2.3절, cert-manager·Rancher·이후 Airflow 설치|
|cert-manager|클러스터 안에서 TLS 인증서를 발급·갱신하는 컨트롤러|2.3절, Rancher 설치 전제 조건|
|Harbor|사내에 두는 컨테이너 이미지 레지스트리. 프로젝트·권한·robot 계정 지원|3장|
|GitLab Runner|GitLab CI 파이프라인의 job을 실제로 실행하는 에이전트|4장|
|DinD|Docker-in-Docker. CI 컨테이너 안에서 별도 Docker 데몬을 띄워 이미지를 빌드하는 방식|4.2절|
|containerd|RKE2가 쓰는 컨테이너 런타임. 레지스트리 신뢰 설정을 Docker와 다른 곳에서 한다|3.3절|

**프로젝트 개요**

|항목|내용|
|---|---|
|기간|2025.01 ~ 2025.08 (8개월, PoC → 구축)|
|역할|설계·구축·운영 주도 (사내 장애예방 과제 수행)|
|규모|IDC 서버 4대 → 이후 AWS EC2 워커를 붙여 하이브리드로 확장|
|스택|Rocky Linux 8.10, RKE2 v1.31, Rancher, cert-manager, Helm 3, Harbor, GitLab CE, GitLab Runner(Docker executor + DinD)|

---

이 시리즈는 사내 데이터 수집 배치를 쿠버네티스 위로 옮긴 프로젝트를 두 편으로 나눠 정리한 기록이다.

- **1편 (이 글)**: 실행 플랫폼. 서버 준비부터 RKE2 클러스터, Helm, Harbor, GitLab CI까지 "컨테이너를 돌릴 땅"을 만든 과정
- **[2편](/kubernetes/airflow-on-k8s-gitsync-kpo/)**: 그 위에 Airflow를 Helm으로 올리고, git-sync로 DAG을 배포하고, KubernetesPodOperator(KPO)로 task를 Pod 단위로 실행한 과정

배치 실행 시간 단축 같은 성과는 대부분 2편의 파이프라인 재설계에서 나왔다. 프로젝트 기간 8개월도 대부분은 플랫폼 구축이 아니라 2편에서 다룰 레거시 코드 컨테이너화에 들어갔다. 이 글은 그 재설계가 돌아갈 수 있는 **자원과 표준 실행 환경**을 어떻게 만들었는지에 집중한다.

---

## 1. 배경: 왜 클러스터가 필요했나

### 1.1 AS-IS

데이터 수집 배치가 서버 3대에 흩어져 있었다.

|구분|AS-IS|문제|
|---|---|---|
|인프라|서버 단위 운영, 한 서버에 여러 서비스가 섞여 있음|자원이 남는 서버와 모자라는 서버가 따로 놂|
|실행 방식|서버마다 Airflow(CeleryExecutor) + Python 가상환경|환경 재현이 안 되고, 서버를 옮기려면 처음부터 다시 설치|
|배포|서버에 접속해 수동 배포, CI/CD 없음|버전 관리·롤백 불가|
|OS|CentOS 7 (지원 종료)|보안 패치·유지보수 리스크|

### 1.2 병렬화 한계 실측

클러스터로 가기 전에 "기존 서버에서 병렬도를 올리면 되지 않나"부터 확인했다.

- 기존 서버: 24 core / 125GB, Celery worker 16개
- 작업 두 개만 동시에 돌려도 PDF 생성 단계에서 CPU가 포화됐다
- 시간대를 나눠 돌리면 해결은 되지만, 그만큼 DAG과 스케줄 설정이 늘어난다

결론은 **서버 한 대 안에서 worker를 늘리는 방식은 한계가 분명하고, 여러 서버를 하나의 자원 풀로 묶어 task를 나눠 뿌리는 구조가 필요하다**였다. 이게 쿠버네티스를 도입한 출발점이다.

### 1.3 목표

- 서버 여러 대의 CPU·메모리를 하나의 클러스터로 묶는다
- 작업을 컨테이너 이미지로 만들어 어느 노드에서든 같은 환경으로 돌게 한다
- 코드 push만 하면 이미지가 빌드되어 사내 레지스트리에 쌓이는 표준 경로를 만든다

---

## 2. 클러스터 구축: RKE2 + Rancher

### 2.1 설계 결정

**배포판: kubeadm vs RKE vs RKE2**

|기준|kubeadm|RKE|RKE2 (선택)|
|:---|:---|:---|:---|
|런타임|직접 구성|Docker 기반|containerd 내장|
|설치 난이도|컴포넌트를 하나씩 직접 구성|쉬움|바이너리 하나 + systemd 서비스|
|앞으로의 지원|표준|Docker 의존이라 불리|Rancher의 주력 배포판|
|관리 UI|별도|Rancher|Rancher|

RKE2를 고른 이유는 두 가지였다. containerd 기반이라 Docker 의존성에서 자유롭고, Rancher와 묶으면 팀원이 kubectl을 몰라도 웹 UI에서 Secret·워크로드를 관리할 수 있다. 플랫폼을 혼자 만들더라도 쓰는 사람은 팀 전체이기 때문에 두 번째 이유가 컸다.

**서버 역할 분리**

```
                ┌──────────────────────────────┐
                │ 서버 A  Control Plane         │
                │  RKE2 server (etcd)           │
                │  Rancher · cert-manager       │
                │  Prometheus / Grafana         │
                └───────────────┬──────────────┘
                                │ 9345 / 6443
              ┌─────────────────┴─────────────────┐
     ┌────────▼────────┐                 ┌────────▼────────┐
     │ 서버 B  Worker   │                 │ 서버 C  Worker   │
     │ RKE2 agent       │                 │ RKE2 agent       │
     └────────┬────────┘                 └────────┬────────┘
              │       image pull (443)            │
              └──────────────────┬────────────────┘
                     ┌───────────▼───────────┐
                     │ 서버 D  공통 저장소     │  ← 클러스터 밖
                     │ GitLab · GitLab Runner │
                     │ Harbor · PostgreSQL    │
                     └───────────────────────┘
```

위 구성은 서버 4대를 "클러스터 안(제어·실행)"과 "클러스터 밖(코드·이미지 저장)"으로 나눈 모습이다. GitLab과 Harbor를 클러스터 밖에 둔 이유는, 클러스터에 문제가 생겨도 코드와 이미지는 살아 있어야 클러스터를 다시 세울 수 있기 때문이다. 또 이 서버에는 이미 GitLab이 돌고 있어서 재활용하는 편이 현실적이었다.

**이관 순서**

새 EC2에 기존 서비스를 백업해 두고 옮기는 방법도 검토했지만, 같은 일을 두 번 하게 된다고 판단했다. 대신 서버 A에 클러스터를 먼저 세우고, 배치 주기가 주 단위라 여유가 있는 서버의 작업부터, 그다음 매일 도는 서버의 작업 순으로 옮겼다.

### 2.2 서버 준비

기존 서버는 Hadoop 생태계가 깔려 있던 CentOS 7이라 포트를 잡고 있는 프로세스가 많았다. 백업 대상을 고른 뒤 OS부터 새로 설치했다.

- **백업 대상 선정**: `/data`(주 데이터), `/etc`(설정)는 필수, `/home`·`/var/lib`는 필요할 때만. 대부분의 결과물은 이미 S3에 있어서 실제로 옮길 양은 많지 않았다
- **OS**: CentOS 7 → Rocky Linux 8.10
- **쿠버네티스 필수 설정**: swap off, chrony 시간 동기화, 방화벽 점검

```bash
# kubelet은 swap이 켜져 있으면 기본 설정으로 뜨지 않는다
swapoff -a
sed -i '/ swap / s/^/#/' /etc/fstab   # 재부팅 후에도 유지

# 서버팀에 요청하기 전에 직접 확인한 기본 항목
ip addr; ip route; cat /etc/resolv.conf     # 네트워크
hostnamectl; cat /etc/os-release            # 호스트명·OS
firewall-cmd --list-all                     # 방화벽
lsblk; df -h                                # 디스크
nc -zv <대상_IP> <포트>                       # 서비스 간 통신
```

### 2.3 컨트롤 플레인 설치

RKE2는 설치 스크립트 하나로 containerd, kubelet, 쿠버네티스 컴포넌트가 함께 깔리고 systemd 서비스로 등록된다.

```bash
# server(컨트롤 플레인) 설치
curl -sfL https://get.rke2.io | INSTALL_RKE2_TYPE="server" sh -
systemctl enable --now rke2-server.service
journalctl -u rke2-server -f        # 기동 로그 확인

# kubectl 사용 설정 — RKE2는 바이너리를 자체 경로에 둔다
mkdir -p ~/.kube
cp /etc/rancher/rke2/rke2.yaml ~/.kube/config
echo 'export PATH=$PATH:/var/lib/rancher/rke2/bin' >> ~/.bashrc
echo 'source <(kubectl completion bash); alias k=kubectl; complete -F __start_kubectl k' >> ~/.bashrc
source ~/.bashrc

kubectl get nodes
```

워커가 붙을 때 쓸 조인 토큰은 `/var/lib/rancher/rke2/server/node-token`에 생긴다.

### 2.4 Helm → cert-manager → Rancher

Rancher는 Helm 차트로 설치하고, Rancher가 쓰는 TLS 인증서 처리를 위해 cert-manager가 먼저 있어야 한다. 그래서 순서는 **Helm → cert-manager → Rancher**다.

```bash
# 1) Helm — 외부 접근이 막혀 있으면 릴리스 tar를 받아 옮긴다
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh && ./get_helm.sh

# 2) cert-manager — CRD까지 차트로 함께 설치
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true

# 3) Rancher — 사내 사설 CA로 서명한 인증서를 Secret으로 넣는다
kubectl create namespace cattle-system
kubectl -n cattle-system create secret tls tls-rancher-ingress \
  --cert=server.crt --key=server.key
kubectl -n cattle-system create secret generic tls-ca \
  --from-file=cacerts.pem=./ca.crt

helm repo add rancher-stable https://releases.rancher.com/server-charts/stable
helm upgrade --install rancher rancher-stable/rancher \
  --namespace cattle-system \
  --set hostname=rancher.example.internal \
  --set replicas=1 \
  --set ingress.tls.source=secret \
  --set privateCA=true
```

처음 PoC에서는 `nip.io` 도메인과 Let's Encrypt로 접속을 시도했지만, 사내망에서는 외부 인증 기관의 검증(HTTP-01)이 닿지 않는다. 결국 **사내 인증서를 Secret으로 넣고 `privateCA=true`로 설치**하는 방식으로 정리했다. 이후 사내 와일드카드 인증서가 커버하는 도메인 범위에 맞게 Rancher 도메인 구조도 한 번 조정했다.

### 2.5 워커 노드 조인

워커는 같은 스크립트를 `agent` 타입으로 실행하고, 컨트롤 플레인 주소와 토큰만 알려 주면 된다. **버전은 server와 반드시 맞춘다.**

```bash
curl -sfL https://get.rke2.io | \
  INSTALL_RKE2_TYPE="agent" INSTALL_RKE2_VERSION="v1.31.6+rke2r1" sh -

mkdir -p /etc/rancher/rke2
cat <<EOF > /etc/rancher/rke2/config.yaml
server: https://<CONTROL_PLANE_IP>:9345
token: <NODE_TOKEN>
EOF

firewall-cmd --permanent --add-port=9345/tcp --add-port=10250/tcp
firewall-cmd --reload
systemctl enable --now rke2-agent.service
```

Rancher가 설치된 `local` 클러스터에는 UI로 노드를 등록할 수 없어서 rke2-agent를 직접 조인해야 한다는 점은 미리 알아 두면 좋다.

**AWS 워커를 붙일 때 (하이브리드 확장)**

운영 중에 외부 프록시가 필요한 수집 작업이 생겨 AWS EC2를 워커로 붙였다. 노드 조인 자체는 같은데 두 가지가 달랐다.

1. Amazon Linux에서는 설치 스크립트가 RPM 방식으로 `rke2-selinux`를 설치하려다 `container-selinux` 의존성 충돌로 실패했다. `--skip-broken`으로 넘기지 않고 `INSTALL_RKE2_METHOD="tar"`로 tarball 설치를 강제해 해결했다
2. IDC 방화벽과 AWS 보안그룹을 **양방향으로** 열어야 했다

|방향|포트|용도|막히면 생기는 증상|
|:---|:---|:---|:---|
|워커 → 컨트롤 플레인|TCP 9345|RKE2 agent 등록|agent 로그에 CA 인증서 timeout|
|워커 → 컨트롤 플레인|TCP 6443|kube-apiserver|노드가 NotReady|
|컨트롤 플레인 → 워커|TCP 10250|kubelet (logs, exec)|`kubectl logs/exec`만 실패|
|양방향|UDP 8472|Flannel VXLAN (Canal CNI)|노드는 Ready인데 Pod끼리 통신·DNS 실패|

UDP는 `nc -zv`로 확인이 안 되기 때문에 한쪽에서 `tcpdump -nn -i any udp port 8472`를 켜 두고 반대편에서 `echo test | nc -u <IP> 8472`로 패킷이 도착하는지 확인했다. 이 표는 이후 노드를 추가할 때마다 쓰는 체크리스트가 됐다.

### 2.6 스토리지: local-path로 시작

PVC를 쓰는 앱(Airflow 등)을 올리려면 StorageClass가 필요해서, 가장 간단한 `local-path-provisioner`로 시작했다.

```bash
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.31/deploy/local-path-storage.yaml
kubectl patch storageclass local-path \
  -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
```

default StorageClass로 지정해 두면 차트의 `storageClassName`을 비워 둬도 자동으로 바인딩된다. 다만 local-path는 PVC가 처음 바인딩된 노드의 디스크에 고정되기 때문에, 워커가 늘어난 뒤에는 "Pod가 특정 노드에만 뜨는" 문제가 생겼다. 이 부분은 나중에 NFS(RWX)로 전환했다(6장 회고).

---

## 3. 사설 이미지 레지스트리: Harbor

### 3.1 왜 Harbor인가

- 서버가 외부 레지스트리(Docker Hub 등)에 자유롭게 나갈 수 없는 폐쇄망 환경이었다
- 작업 이미지에 사내 코드가 들어가므로 외부에 올릴 수 없다
- Harbor는 프로젝트 단위 권한, robot 계정, 웹 UI를 제공해서 "팀 공용 레지스트리"로 쓰기 좋았다

### 3.2 설치 (공통 저장소 서버, docker-compose)

Harbor는 클러스터 밖의 공통 저장소 서버에 오프라인 설치 패키지로 올렸다. 이 서버는 CentOS 7에 Docker 1.13이 깔린 오래된 환경이라 설치 자체가 제일 큰 산이었다.

```bash
# 1) 레지스트리 도메인으로 인증서 발급 (CN과 SAN을 실제 접속 도메인과 일치시킨다)
openssl req -newkey rsa:4096 -nodes -sha256 \
  -keyout harbor.key -x509 -days 365 \
  -subj "/CN=harbor.example.internal" \
  -addext "subjectAltName=DNS:harbor.example.internal" \
  -out harbor.crt

# 2) harbor.yml 에 hostname, https 인증서 경로 지정 후 설치
./install.sh
```

여기서 만난 문제들은 이렇다.

|에러|원인|해결|
|:---|:---|:---|
|`x509: certificate is valid for A, not B`|인증서를 만들 때 CN을 접속 도메인과 다르게 입력|CN·SAN을 실제 도메인으로 재발급|
|`docker-compose-plugin` 설치 실패|Docker 버전이 너무 오래됨|다른 서버에서 docker-compose 바이너리를 받아 옮기고 심볼릭 링크|
|yum이 Python 버전 충돌로 동작 안 함|시스템 Python 2.7 의존 패키지 충돌|yum 스크립트의 인터프리터를 잠시 바꿔 설치 후 원복|
|80 포트 바인딩 실패|기존 Apache httpd가 80을 점유|httpd 중지·비활성화|
|`lookup core ... server misbehaving`, 로그인 502|같은 서버에서 돌던 GitLab의 redis 등과 컨테이너 이름이 충돌|Harbor 컨테이너 이름 분리 후 재기동|
|로그인 401 `Invalid credentials`|이전 시도에서 바꾼 비밀번호가 DB 볼륨에 남아 있음|데이터 볼륨을 새로 잡아 초기화|

마지막 두 개는 원인 하나를 고치면 다음 원인이 드러나는 식이라 시간이 꽤 걸렸다. 로그를 컨테이너별로(`/var/log/harbor/core.log`) 나눠 보는 습관이 여기서 생겼다.

### 3.3 노드가 Harbor를 믿게 만들기

Harbor는 떴는데, 이번에는 새로 붙인 워커에서 Pod가 `ErrImagePull`로 실패했다. 원인은 두 가지였다.

1. 워커가 Harbor 도메인을 해석하지 못함 → `/etc/hosts` 등록 (사내 DNS에 없는 도메인이라)
2. 사설 인증서를 신뢰하지 못함 (`x509: certificate signed by unknown authority`)

RKE2는 Docker가 아니라 **containerd**를 쓰기 때문에 `/etc/docker/certs.d`가 아니라 RKE2 쪽 경로에 신뢰 설정을 해야 한다.

```bash
# 처음에는 /etc/rancher/rke2/registries.yaml 의 insecure_skip_verify 로 우회했다가,
# CA를 노드에 배포하는 방식으로 바꿨다
mkdir -p /var/lib/rancher/rke2/agent/etc/containerd/certs.d/harbor.example.internal
cp ca.crt /var/lib/rancher/rke2/agent/etc/containerd/certs.d/harbor.example.internal/ca.crt
systemctl restart rke2-agent
```

Pod가 이미지를 pull할 때 쓰는 인증 정보는 Harbor **robot 계정**으로 `imagePullSecret`을 만들어 작업용 네임스페이스에 두었다. admin 계정은 외부 연동에서 막히는 경우가 있었고, 권한을 프로젝트 단위로 좁힐 수 있다는 점에서도 robot 계정이 맞았다.

```bash
kubectl -n <작업_네임스페이스> create secret docker-registry harbor-auth \
  --docker-server=harbor.example.internal \
  --docker-username='robot$<프로젝트>+<이름>' \
  --docker-password=<ROBOT_TOKEN>
```

> **신규 노드 체크리스트에 들어간 것**: Harbor 도메인 해석, containerd `certs.d`에 CA 배포, rke2-agent 버전 일치, 방화벽 4종(9345/6443/10250 TCP, 8472 UDP).

---

## 4. 이미지 CI: GitLab Runner + DinD

### 4.1 목표 흐름

```
개발자 git push (코드 + Dockerfile + .gitlab-ci.yml)
   │
   ▼
GitLab ──▶ GitLab Runner (Docker executor)
              └─ job 컨테이너 안에서 docker:dind 서비스로 이미지 빌드
                    │ docker push
                    ▼
                 Harbor ──(imagePull)──▶ 쿠버네티스 Pod
```

이 흐름이 생기면서 "서버에 들어가서 코드를 고치고 다시 돌리는" 배포가 "push하면 이미지가 쌓이고, 클러스터는 이미지를 받아 실행하는" 구조로 바뀌었다. 실행 환경이 이미지에 고정되니 로컬과 운영 환경이 같아지고, 이전 이미지로 되돌리는 것도 가능해졌다.

### 4.2 왜 DinD였나

Runner를 Docker executor로 쓰면 이미지를 빌드하는 방법은 크게 두 가지다.

|방식|장점|단점|이 환경에서|
|:---|:---|:---|:---|
|호스트 `docker.sock` 마운트|빠르고 설정이 간단|호스트 Docker를 그대로 씀|호스트 Docker가 1.13이라 최신 빌드 기능을 쓸 수 없음|
|DinD (선택)|job마다 독립된 최신 Docker 데몬|`privileged` 필요|호스트 버전과 무관하게 `docker:24.0.7`로 빌드 가능|

호스트 Docker가 너무 오래되어서 사실상 DinD밖에 선택지가 없었다. 대신 `privileged = true`가 필요하므로, 이 Runner는 신뢰할 수 있는 사내 그룹 프로젝트 전용 **그룹 러너**로만 등록했다.

### 4.3 Runner 설정 (`config.toml`)

```toml
[[runners]]
  name = "group-runner"
  url = "https://gitlab.example.internal/"
  executor = "docker"
  [runners.docker]
    image = "docker:24.0.7"
    privileged = true                       # DinD 필수
    # volumes 에 /var/run/docker.sock 을 마운트하지 않는다 (DinD와 충돌)
    pull_policy = ["if-not-present"]        # 폐쇄망: 미리 준비한 이미지 사용
    allowed_pull_policies = ["if-not-present"]
    extra_hosts = ["harbor.example.internal:<HARBOR_IP>"]  # job 컨테이너에서 Harbor 이름 해석
    shm_size = 4294967296
```

### 4.4 파이프라인 (`.gitlab-ci.yml`)

Harbor 주소·계정은 저장소에 두지 않고 **GitLab 그룹 CI/CD 변수**로 등록해 모든 프로젝트가 같은 값을 쓰게 했다.

```yaml
stages: [build]

variables:
  DOCKER_HOST: "tcp://docker:2375"   # dind 서비스에 연결
  DOCKER_TLS_CERTDIR: ""             # dind TLS 비활성화 (내부망 전용)
  IMAGE_NAME: $HARBOR_REGISTRY/$HARBOR_PROJECT/my-job

build-and-push:
  stage: build
  tags: [data]                       # 그룹 러너의 태그와 일치해야 job이 잡힌다
  image:
    name: docker:24.0.7
    pull_policy: if-not-present
  services:
    - name: docker:24.0.7-dind
      alias: docker
      command: ["--tls=false", "--host=tcp://0.0.0.0:2375",
                "--insecure-registry=harbor.example.internal"]
  before_script:
    - docker info
    - echo "$HARBOR_PASSWORD" | docker login $HARBOR_REGISTRY -u $HARBOR_USERNAME --password-stdin
  script:
    - docker build -t $IMAGE_NAME:$CI_COMMIT_SHORT_SHA .
    - docker tag $IMAGE_NAME:$CI_COMMIT_SHORT_SHA $IMAGE_NAME:latest
    - docker push $IMAGE_NAME:$CI_COMMIT_SHORT_SHA
    - docker push $IMAGE_NAME:latest
```

### 4.5 흔한 에러 → 원인 → 해결

|에러|원인|해결|
|:---|:---|:---|
|job이 pending에서 안 넘어감|그룹 러너에 태그가 달려 있는데 job에 `tags`가 없음|job에 러너 태그 추가|
|`failed to pull image 'docker:dind'`|폐쇄망이라 Docker Hub에 못 나감|인터넷 되는 서버에서 `docker save` → 옮겨서 `docker load`. 이후 기본 이미지는 Harbor에 올려 두고 사용|
|`pull_policy ([never]) ... not one of the allowed_pull_policies ([always])`|파이프라인과 Runner의 pull policy 불일치|Runner에 `allowed_pull_policies = ["if-not-present"]` 지정|
|`Cannot connect to the Docker daemon at unix:///var/run/docker.sock`|DinD 구성인데 job이 소켓으로 붙으려 함|`DOCKER_HOST=tcp://docker:2375`, Runner `privileged = true`|
|DinD 안에서 Harbor 도메인을 못 찾음|job 컨테이너는 호스트의 `/etc/hosts`를 모름|Runner `extra_hosts`에 Harbor 등록|

`docker.sock` 방식으로 한참 헤매다가 DinD로 돌아온 게 이 단계의 가장 큰 교훈이었다. 우회 방법을 찾기 전에 **"이 환경의 제약(구형 Docker, 폐쇄망)에서 가능한 구조가 무엇인가"부터 정했어야** 했다.

### 4.6 표준화: 신규 작업 온보딩 4단계

플랫폼이 갖춰진 뒤에는 새 작업을 올리는 절차를 문서로 고정했다.

1. **도커라이징**: 환경 의존 로직 제거, 설정값은 환경 변수로, Dockerfile 작성
2. **이미지 CI**: Harbor 프로젝트 생성, `.gitlab-ci.yml` 추가 (그룹 러너·그룹 변수 재사용)
3. **실행 정의 추가**: Airflow DAG에 이미지와 실행 인자 지정 (2편)
4. **Secret 구성**: 접속 정보는 Rancher에서 작업별 Secret으로 등록

이후 다른 팀원의 신규 파이프라인도 별도 서버를 만들지 않고 이 절차대로 같은 클러스터에 올라갔다. 플랫폼이 "내 프로젝트용"에서 "팀 공용"이 된 시점이다.

---

## 5. 운영하며 확인한 RKE2의 함정

구축이 끝이 아니었다. 1년 넘게 운영하면서 구축 단계에서는 안 보이던 문제들이 나왔다.

|증상|원인|조치|
|:---|:---|:---|
|PoC 중 Pod 대량 Evicted, Rancher 접속 불가|이미지가 큰데 루트 파티션이 30GB만 잡혀 있었음|`growpart` + `xfs_growfs`로 루트 확장, 이미지 경량화|
|`ContainerCreating`에서 멈춤, `Failed to garbage collect required amount of images`|노드 디스크가 차서 kubelet 이미지 GC가 동작했지만 지울 수 있는 이미지가 없음|dangling 이미지(`crictl rmi --prune`)·테스트 리소스 정리, 디스크 알림 추가|
|개인 PC에서 kubectl 인증 실패|복사해 둔 kubeconfig의 클라이언트 인증서가 만료|RKE2가 관리하는 kubeconfig를 다시 받아 사용|
|노드 다운 → 스케줄러 중단|**노드가 오래 재시작 없이 돌아 RKE2 인증서 자동 갱신 시점을 놓침**|인증서 갱신 후 복구, 만료 메트릭 알림과 정기 재시작 절차 도입|

마지막 건이 가장 컸다. RKE2는 **서비스가 재시작될 때 만료가 가까운 인증서를 자동으로 갱신**한다. 바꿔 말하면, 1년 넘게 한 번도 재시작하지 않은 노드는 갱신 기회 자체가 없다. "안정적으로 오래 떠 있다"가 오히려 장애의 원인이 된 셈이다. 이후 `apiserver_client_certificate_expiration_seconds` 기반 Slack 알림을 걸고, 배치가 없는 시간대에 정기 재시작하는 절차를 만들었다. 이 장애 대응 과정(DNS 구조 개선, NFS 전환, 모니터링)은 별도 글로 정리할 예정이다.

---

## 6. 회고: 다시 한다면

|한계|왜 그렇게 됐나|다시 한다면|
|:---|:---|:---|
|컨트롤 플레인 1대, etcd 단일|서버 수 제약, PoC 구조를 그대로 운영으로 가져감|컨트롤 플레인 3대 HA, etcd 스냅샷을 외부에 백업|
|공통 저장소 서버에 GitLab·Harbor·DB가 몰림, 구형 OS|기존 서버 재활용|역할 분리, OS 교체, 백업·복구 리허설|
|local-path로 시작해 노드 고정 문제 발생|가장 빠르게 시작할 수 있는 선택|처음부터 RWX 공유 스토리지(NFS 등)를 기본 StorageClass로|
|Helm values·Runner 설정이 UI와 수동 수정으로 반영됨|빠른 구축 우선|values·매니페스트·Runner 설정을 Git으로 관리 (GitOps는 검토 후 규모 대비 보류)|
|운영에서 `latest` 태그 의존|초기 파이프라인을 단순하게 유지|커밋 SHA 태그로 배포하고 `latest`는 편의용으로만|
|인증서 만료를 장애로 처음 알게 됨|재시작 기반 자동 갱신을 몰랐음|구축 첫날부터 만료 알림과 정기 재시작 일정|

---

## 7. 실무 체크리스트

- [ ] 모든 노드에서 swap이 꺼져 있고, 재부팅 후에도 유지되는가.
- [ ] server와 agent의 RKE2 버전이 일치하는가.
- [ ] 노드 간 9345/6443/10250(TCP), 8472(UDP)가 양방향으로 열려 있는가.
- [ ] 모든 노드가 사설 레지스트리 도메인을 해석하고, containerd에 CA가 배포되어 있는가.
- [ ] 이미지 pull은 admin이 아니라 프로젝트 범위의 robot 계정으로 하는가.
- [ ] 레지스트리 계정이 저장소가 아니라 GitLab CI/CD 변수(masked)로 관리되는가.
- [ ] DinD를 쓰는 privileged 러너가 신뢰된 프로젝트에만 열려 있는가.
- [ ] 노드 디스크 사용률과 인증서 만료일에 알림이 걸려 있는가.
- [ ] 장기 무중단 노드에 정기 재시작(인증서 갱신) 일정이 있는가.

---

## 8. 정리

- **실측으로 출발했다.** 서버 한 대에서 두 작업만 돌려도 CPU가 포화되는 것을 확인하고, worker를 늘리는 대신 서버를 묶는 쪽을 택했다.
- **운영할 사람을 기준으로 도구를 골랐다.** RKE2 + Rancher로 팀원이 kubectl 없이도 클러스터를 다룰 수 있게 했다.
- **코드와 이미지는 클러스터 밖에 뒀다.** GitLab과 Harbor를 분리해 클러스터를 다시 세워도 복구할 재료가 남게 했다.
- **제약이 구조를 정했다.** 폐쇄망·구형 Docker·사설 인증서 때문에 DinD, if-not-present, containerd CA 배포라는 형태가 됐다.
- **구축보다 운영이 길었다.** 인증서 자동 갱신, 이미지 GC처럼 오래 운영해야 보이는 문제를 체크리스트와 알림으로 바꿨다.

[다음 편](/kubernetes/airflow-on-k8s-gitsync-kpo/)에서는 이 클러스터 위에 Airflow를 Helm으로 올리고, git-sync와 KubernetesPodOperator로 배치를 Pod 단위로 실행한 과정을 정리한다.

---

## References

설치 절차와 설정 항목은 각 도구의 공식 문서를 기준으로 했고, 에러 사례와 판단 근거는 프로젝트 진행 중 직접 기록한 작업 일지를 바탕으로 했다. 사내 호스트명·주소·도메인은 예시 값으로 바꿨다.

**쿠버네티스·RKE2·Rancher**

- RKE2 — [Quick Start](https://docs.rke2.io/install/quickstart)
- RKE2 — [Requirements (네트워크 포트)](https://docs.rke2.io/install/requirements)
- RKE2 — [Private Registry Configuration](https://docs.rke2.io/install/private_registry)
- Rancher — [Install/Upgrade on a Kubernetes Cluster](https://ranchermanager.docs.rancher.com/getting-started/installation-and-upgrade/install-upgrade-on-a-kubernetes-cluster)
- Rancher — [local-path-provisioner](https://github.com/rancher/local-path-provisioner)
- Kubernetes — [Pull an Image from a Private Registry](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/)

**Helm·cert-manager**

- Helm — [Installing Helm](https://helm.sh/docs/intro/install/)
- cert-manager — [Helm 설치](https://cert-manager.io/docs/installation/helm/)

**Harbor·GitLab**

- Harbor — [Harbor Installation and Configuration](https://goharbor.io/docs/main/install-config/)
- Harbor — [Configure HTTPS Access to Harbor](https://goharbor.io/docs/main/install-config/configure-https/)
- Harbor — [Robot Accounts](https://goharbor.io/docs/main/administration/robot-accounts/)
- GitLab — [Use Docker to build Docker images (DinD)](https://docs.gitlab.com/ci/docker/using_docker_build/)
- GitLab Runner — [Advanced configuration (config.toml)](https://docs.gitlab.com/runner/configuration/advanced-configuration/)
