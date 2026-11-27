---
title: "데이터 엔지니어의 AWS: S3·Lambda·Glue·Step Functions·SQS를 어디에 썼나"
excerpt: "여러 데이터 파이프라인에서 직접 써 본 AWS 자원을 프로젝트가 아니라 역할 기준으로 정리했다. 모든 파이프라인의 경계가 된 S3, 작업 성격에 따라 EC2·Lambda·Glue·EMR Serverless를 고른 기준, Glue Catalog와 Athena로 수집 결과를 SQL로 보는 구조, EventBridge·Step Functions 스케줄링, 그리고 API 요청 큐와 Snowpipe 트리거라는 SQS의 두 가지 쓰임과 각각의 함정을 다룬다."
categories: [aws]
tags: [aws, s3, lambda, glue, athena, step-functions, eventbridge, sqs, snowpipe, data-engineering, 포트폴리오]
date: 2026-11-27 09:00:00 +0900
last_modified_at: 2026-11-27
---

## 이 글을 30초에 파악하기

> **한 줄로** — 데이터 엔지니어가 파이프라인을 만들 때 손이 가는 AWS 자원을 "무엇을 하는 자리에 무엇을 썼나" 기준으로 정리하고, 실제로 걸려 넘어진 지점을 함께 적은 기록이다.

**이 글에서 보여주는 것**

- 저장·실행·스케줄·메시징·조회 역할별로 쓴 AWS 자원 한눈에 보기
- S3를 파이프라인 경계로 쓸 때의 파일 형식, 이벤트, 개인정보 기준
- 크롤러·변환·Spark 작업마다 EC2 / Lambda / Glue / EMR Serverless를 고른 기준
- EventBridge cron 요일 표기처럼 한 번 틀리면 조용히 어긋나는 설정
- SQS를 API 요청 큐로 쓸 때와 S3 → Snowflake 자동 적재 트리거로 쓸 때의 차이와 함정

**용어 먼저 보기**

|용어|한 줄 설명|이 글에서의 역할|
|---|---|---|
|S3 이벤트 알림|객체가 만들어지거나 지워질 때 SQS·Lambda 등으로 알림을 보내는 기능|파일 도착을 파이프라인 시작 신호로 사용|
|Glue Data Catalog|S3 파일을 테이블처럼 보이게 하는 메타데이터 저장소|Athena·Glue·EMR이 같은 스키마를 공유|
|Glue Crawler|S3 파일을 읽어 스키마를 추론하고 Catalog에 테이블을 만드는 작업|수집 결과 스키마 반영|
|Step Functions|여러 AWS 작업을 상태 기계로 엮는 서버리스 오케스트레이터|수집 → 변환 → 스키마 반영 순서 제어|
|visibility timeout|SQS에서 받은 메시지를 다른 소비자에게 숨겨 두는 시간|"받았다"와 "지웠다"가 다른 이유|
|Snowpipe|스테이지에 들어온 파일을 이벤트 기반으로 `COPY INTO` 하는 Snowflake 기능|S3 → Snowflake 자동 적재|

---

데이터 파이프라인에서 쓰는 AWS 자원을 저장·실행·스케줄·메시징·조회 역할별로 정리한다. EMR Serverless와 RDS는 다른 글([1편](/spark/spark-etl-emr-serverless/), [2편](/rag/hybrid-search-pgvector-bm25/))에서 다뤄서 여기서는 짧게만 언급한다.

---

## 1. 한눈에 보기

|역할|쓴 자원|고른 이유|
|---|---|---|
|저장·경계|S3|원천과 적재 대상 사이에 파일로 끊어 두면 재처리와 추적이 쉬움|
|실행 (짧은 변환)|Lambda|파일 하나 단위의 가벼운 변환, 유휴 비용 없음|
|실행 (장시간·브라우저)|EC2|Selenium처럼 오래 걸리고 브라우저가 필요한 작업|
|실행 (정제·Spark)|Glue job, EMR Serverless|클러스터 관리 없이 Spark 작업 단위로 실행|
|메타데이터·조회|Glue Crawler, Data Catalog, Athena|S3 파일을 서버 없이 SQL로 조회|
|스케줄·순서|EventBridge Scheduler, Step Functions|주기 실행과 단계 간 순서 제어|
|메시징|SQS|요청 버퍼링, S3 이벤트 전달|
|서빙 DB|RDS for PostgreSQL (pgvector)|정형 필터와 벡터 검색을 한 DB에서|
|권한·관측|IAM, CloudWatch|서비스별 최소 권한, 큐·함수 지표와 로그|

---

## 2. S3: 모든 파이프라인의 경계

### 2.1 영역을 나눠 둔다

파일이 들어오는 파이프라인은 S3 안에서 단계를 경로로 나눴다.

```
업로드 / 수집 결과
      │
      ▼
s3://<bucket>/raw/...        원본 그대로. 다시 처리할 일이 생기면 여기서 시작
      │  검증·정제 (Lambda / Glue)
      ▼
s3://<bucket>/validated/...  형식이 맞는 파일만. 적재 대상이 읽는 곳
      │
      ▼
Athena 조회 / Snowflake COPY INTO / DB 적재
```

원본을 지우지 않고 남겨 두면, 정제 로직을 고쳤을 때 원천 시스템을 다시 건드리지 않고 raw부터 다시 돌릴 수 있다. 적재 대상에서 문제가 보였을 때 "어느 파일에서 왔는지"를 거슬러 올라갈 수 있는 것도 이 구조 덕분이다.

### 2.2 CSV보다 Parquet

처음에는 CSV(gzip)로 내리고 `COPY INTO`로 올렸는데, 적재 에러의 대부분이 텍스트 형식 문제였다.

|에러·증상|원인|
|---|---|
|`Found character 'í' instead of field delimiter '\t'`|텍스트 값 안에 구분자·특수문자가 섞여 행이 밀림|
|`PARTIALLY_LOADED`, `User character length limit exceeded`|대상 컬럼 길이보다 긴 값. 에러 행을 건너뛰는 설정이면 일부 행만 들어가고 파일 적재는 끝난다|
|마지막 컬럼 개수 불일치|맨 뒤 비고 컬럼이 빈 값이라 구분자가 하나 빠진 행이 생김|
|파일을 아예 못 읽음|업로드된 엑셀에 문서 암호화(DRM)가 걸려 있었음|

DB에서 S3로 내리는 경로는 Parquet으로 바꿨다. 스키마와 타입이 파일에 같이 들어가니 구분자 문제가 사라지고, 압축률이 좋고, Snowflake가 여러 파일을 병렬로 읽어 `COPY INTO`도 빨랐다.

대신 Parquet을 `MATCH_BY_COLUMN_NAME`으로 적재할 때는 **컬럼 이름이 어긋나도 에러 없이 NULL로 들어간다.** 그래서 대상 테이블 DDL과 추출 쿼리의 컬럼 목록을 한 정의에서 같이 생성해, 둘이 어긋날 수 없게 했다.

### 2.3 민감 컬럼은 추출 단계에서 뺀다

개인정보 컬럼을 적재 대상 테이블 정의에서만 빼면, 그 값은 이미 S3의 Parquet 파일에 평문으로 남아 있다. S3를 경계로 쓰는 구조에서는 **원천에서 추출하는 SELECT 단계**에서 빼야 한다.

### 2.4 재실행해도 결과가 같게

증분 적재는 "최근 30일" 같은 롤링 구간으로 추출하고, 적재 대상에서도 **같은 조건식으로** 그 구간을 지운 뒤 다시 넣었다. 추출 조건과 삭제 조건이 같으니 같은 날 몇 번을 다시 돌려도 결과가 같다.

### 2.5 이벤트는 "생성" 전체를 건다

S3 이벤트 알림을 `s3:ObjectCreated:Put`에만 걸었더니 일부 업로드에 반응하지 않았다. 큰 파일은 멀티파트 업로드로 올라가고, 이때 이벤트는 `CompleteMultipartUpload`로 발생한다. 업로드 방식과 상관없이 받으려면 `s3:ObjectCreated:*`로 건다.

---

## 3. 실행 환경 고르기

### 3.1 기준

|작업|선택|이유|
|---|---|---|
|업로드된 엑셀 한 건을 CSV로 변환|Lambda|파일 하나 단위, 수 초 안에 끝남, 평소에는 비용 0|
|Selenium 동적 크롤링|EC2 + cron|브라우저 로딩을 기다려야 해서 오래 걸림. Lambda는 실행 시간 상한이 15분이라 맞지 않음|
|수집 결과 정제·형식 변환|Glue job|서버 없이 Spark 스크립트를 실행, Data Catalog와 바로 연결|
|여러 테이블 조인·임베딩 배치|EMR Serverless|Python 환경과 Spark 설정을 자유롭게 넘길 수 있음([1편](/spark/spark-etl-emr-serverless/))|

EC2는 같은 서버를 기존 서비스와 같이 쓸지, 새로 띄울지도 따져야 했다.

|선택지|장점|단점|
|---|---|---|
|기존 EC2에 같이 올림|추가 비용 없음|ETL이 자원을 많이 쓰면 기존 서비스가 느려지고, 작업 중 기존 서비스를 건드릴 위험|
|ETL 전용 EC2|기존 서비스와 분리|주 1회 배치를 위해 나머지 시간은 놀고 있음|
|Lambda|유휴 비용 없음|15분 상한. 브라우저 기반 크롤링은 불가|

주 1회 도는 크롤링이라 전용 EC2에 Python 스크립트와 cron을 두는 단순한 구성으로 갔다. 대신 이 구성은 성공·실패를 알려 주는 장치가 없어서, 결과 파일이 생겼는지 직접 확인해야 했다. 지금이라면 작업 끝에 결과 건수를 CloudWatch 지표나 알림으로 남기겠다.

### 3.2 Lambda: 파일 하나를 표준 형식으로

파일 업로드 기반 적재에서 Lambda가 맡는 일은 "사람이 만든 엑셀"을 "DB가 읽을 수 있는 CSV"로 바꾸는 것이었다.

```python
import awswrangler as wr

def handler(event, context):
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        df = wr.s3.read_excel(f"s3://{bucket}/{key}")
        df = normalize(df)    # 컬럼명 통일, "45K" 같은 문자열 금액을 숫자로, 해석 불가 값은 비고 컬럼으로

        wr.s3.to_csv(df, f"s3://{bucket}/validated/{target_of(key)}.csv", index=False)
```

변환 규칙은 단순했지만 "숫자 칸에 숫자가 아닌 값"이 섞이는 경우가 많았다. 해석할 수 없는 값을 버리지 않고 비고 컬럼에 원문을 남겨 두면, 나중에 규칙을 보강할 때 무엇이 빠졌는지 바로 알 수 있다.

### 3.3 Glue job

Glue job은 S3에 둔 PySpark 스크립트를 서버 없이 실행한다. 몇 가지 알아 둘 점이 있었다.

|항목|내용|
|---|---|
|`DynamicFrame`과 `DataFrame`|Glue 전용 `DynamicFrame`은 스키마가 흔들리는 데이터에 강하고, 일반 변환은 `toDF()`로 Spark DataFrame으로 바꿔 쓰는 편이 편했다. `toDF()`를 부를 때 생성자 관련 경고가 뜨는데, 알려진 경고다|
|DPU|Glue는 DPU 단위로 과금된다. 작업 모니터링에서 시간대별 실제 사용 DPU를 볼 수 있어, 할당량이 과한지 판단하는 근거가 된다|

---

## 4. Glue Catalog + Athena: 수집 결과를 SQL로

수집처마다 형식이 다른 데이터를 별도 DB나 서버 없이 바로 SQL로 보려면, S3 위에 메타데이터를 얹는 구조가 간단하다.

```
수집 스크립트 ──▶ S3 raw (JSON)
                   │ Glue job: 전처리·적재
                   ▼
                 S3 validated
                   │ Glue Crawler: 스키마 추론
                   ▼
                 Glue Data Catalog (DB / 테이블 정의)
                   │
                   ▼
                 Athena에서 SQL 조회
```

|결정|이유|
|---|---|
|조회는 Athena|상시 서버가 없고, 스캔한 데이터 양만큼만 과금된다. 조회 빈도가 낮은 데이터에 맞았다|
|스키마는 Crawler가 반영|채널별로 필드가 늘어도 테이블 정의를 손으로 고치지 않아도 된다|
|메타데이터는 Catalog 한곳에|Athena와 Glue job이 같은 테이블 정의를 본다. 조회와 가공이 같은 스키마를 쓴다|

Athena는 스캔한 양이 곧 비용이라, 데이터가 커지면 Parquet 같은 컬럼 형식과 날짜 파티션으로 스캔량을 줄이는 것이 첫 번째 최적화다.

이 단계들을 Step Functions로 묶어 "수집 → 정제 → 스키마 반영"이 순서대로 돌게 했다. 각 단계가 끝나야 다음 단계가 시작되고, 어느 단계에서 멈췄는지 실행 이력에서 바로 보인다.

---

## 5. 스케줄: EventBridge cron은 요일 번호가 다르다

주기 실행은 EventBridge Scheduler의 cron 표현식을 썼다. 리눅스 cron과 비슷해 보이지만 다르다.

```
cron(0 7 ? * 2 *)     # 매주 월요일 07:00
     │ │ │ │ │ └ 연도
     │ │ │ │ └── 요일: 1=일요일 ... 7=토요일  (리눅스 cron은 0 또는 7=일요일, 1=월요일)
     │ │ │ └──── 월
     │ │ └────── 일: 요일을 지정하면 '?'
     │ └──────── 시
     └────────── 분
```

|차이|함정|
|---|---|
|요일 번호가 1=일요일부터|리눅스 습관대로 `1`을 쓰면 월요일이 아니라 일요일에 돈다|
|일과 요일 중 하나는 `?`|둘 다 `*`로 쓰면 표현식이 거부된다|
|필드가 6개(연도 포함)|리눅스 cron 5개 필드를 그대로 붙이면 안 된다|
|시간대|스케줄마다 시간대를 지정할 수 있다. 지정하지 않으면 UTC 기준이라 9시간 어긋난다|

AWS 서비스끼리만 엮는 짧은 흐름은 EventBridge + Step Functions로, 외부 시스템이 섞이고 재실행·백필이 잦은 배치는 Airflow로 나눠 썼다.

---

## 6. SQS: 두 가지 쓰임

### 6.1 API 요청 큐

API 앞에 FIFO 큐를 두고, 요청을 큐에 넣은 뒤 꺼내 처리하는 구조를 붙였다. 처음 붙일 때 만난 에러는 이랬다.

|에러|원인|해결|
|---|---|---|
|`Invalid type for parameter MessageBody ... valid types: <class 'str'>`|요청 객체를 그대로 넣음. 본문은 문자열만 받는다|`json.dumps()`로 직렬화, 꺼낼 때 `json.loads()`|
|`AccessDenied ... sqs:SendMessage`|API 서버 역할에 큐 권한이 없음|해당 큐 ARN에 `SendMessage`·`ReceiveMessage`·`DeleteMessage` 권한 부여|

더 오래 걸린 건 **"받았다"와 "지웠다"가 다르다**는 점이었다.

1. `receive_message`로 받은 메시지는 큐에서 사라지는 게 아니라 visibility timeout 동안 **안 보일 뿐**이다.
2. 한 번에 여러 건을 받으면, 내가 처리할 메시지가 아닌 것도 함께 받아 숨겨진다.
3. 처리하지 않은 메시지는 지우지 않으니, timeout이 지날 때까지 아무도 처리하지 못한다.
4. 처리 중 예외가 나서 삭제 코드에 도달하지 못해도 같은 일이 생긴다.

```python
for message in response.get("Messages", []):
    if message["MessageId"] != my_message_id:
        continue                              # 받았지만 안 지움 → timeout 동안 숨겨짐
    try:
        handle(json.loads(message["Body"]))
    finally:
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
```

이 문제는 로그보다 큐 지표에서 먼저 보였다. CloudWatch에서 `NumberOfMessagesReceived`와 `NumberOfMessagesDeleted`를 나란히 두면, 둘의 차이가 벌어지는 구간이 곧 "받고 안 지운" 구간이다.

|상황|다시 한다면|
|---|---|
|자기 메시지만 골라 처리|한 번에 1건만 받거나, 요청마다 응답을 기다리는 대신 별도 소비자가 처리하고 결과를 저장|
|처리 중 예외|정해진 횟수까지 재수신을 허용하고, 넘으면 Dead-Letter Queue로 격리|
|감지|받은 수와 지운 수의 차이, 가장 오래된 메시지 나이(`ApproximateAgeOfOldestMessage`)에 알람|

### 6.2 S3 → SQS → Snowpipe

파일이 S3에 올라오면 Snowflake 테이블에 자동으로 들어가게 할 때도 SQS를 썼다. 이때는 큐를 직접 만들지 않는다.

```
업로드 ─▶ S3 raw ─▶ Lambda (엑셀 → CSV) ─▶ S3 validated
                                              │ S3 이벤트 알림
                                              ▼
                                   Snowflake가 관리하는 SQS
                                              │
                                              ▼
                                   Snowpipe: COPY INTO 테이블
```

|단계|하는 일|
|---|---|
|Storage Integration|Snowflake가 S3를 읽을 수 있게 IAM 역할을 연결|
|`CREATE PIPE ... AUTO_INGEST = TRUE`|Snowflake 쪽 계정에 Snowpipe용 SQS 큐가 자동으로 생긴다|
|`SHOW PIPES`의 `notification_channel`|그 큐의 ARN. S3 버킷 이벤트 알림의 대상으로 이 ARN을 지정|

**INSERT가 아니라 COPY INTO인 이유**: `INSERT`는 행 단위로 소량을 넣거나 고치는 용도다. `COPY INTO`는 스테이지의 파일을 대량으로 병렬 로딩하고, 파일 형식·에러 처리 옵션을 지원한다. 파일로 들어오는 데이터라면 S3에 떨군 뒤 `COPY INTO` 하는 편이 맞고, 어느 파일에서 몇 건이 들어왔는지 적재 이력도 Snowflake에 남는다.

운영하며 만난 문제는 두 가지였다.

|증상|원인|
|---|---|
|업로드 후 테이블 반영까지 지연|이벤트 기반이라도 완전 실시간이 아니다. 대체로 1분 안팎 걸렸다|
|**같은 파일명으로 다시 올리면 반영되지 않음**|Snowpipe는 이미 적재한 파일을 적재 이력으로 기억하고 다시 읽지 않는다. SQS가 이벤트를 안 보낸 게 아니다|

두 번째가 까다로웠다. `FORCE = TRUE`를 주면 다시 읽지만 같은 행이 중복으로 들어가고, "비우고 다시 넣기(`TRUNCATE` + `COPY INTO`)"는 Snowpipe 안에서 할 수 없다. 그래서 파일명에 고유 값을 붙여 매번 새 파일로 올리고, 원래 파일명 기준으로 가장 최근 파일만 보여 주는 뷰를 두는 방식을 검토했다. 파일을 "덮어쓰는" 업로드 방식과 "새 파일만 읽는" 적재 방식이 부딪히는 지점이라, 설계 초기에 업로드 규칙부터 정해 두는 게 좋다.

---

## 7. RDS와 나머지

- **RDS for PostgreSQL**: 벡터 검색용으로 썼다. 엔진 마이너 버전마다 지원하는 pgvector 버전이 달라서, 버전은 필요한 벡터 타입 기준으로 골랐다. 원천에서 재구축할 수 있는 데이터라 Multi-AZ 대신 Single-AZ로 갔다([2편](/rag/hybrid-search-pgvector-bm25/) 5.2).
- **IAM**: 서비스마다 역할을 따로 두고, 필요한 리소스 ARN과 액션만 열었다. 큐 권한 에러(6.1)와 Snowflake Storage Integration(6.2)이 모두 여기서 막혔다.
- **CloudWatch**: Lambda 로그와 SQS 지표를 봤다. SQS 문제는 로그보다 받은 수·지운 수 지표에서 먼저 보였다(6.1).

---

## 8. 회고: 다시 한다면

|한계|왜 그렇게 됐나|다시 한다면|
|---|---|---|
|EC2 + cron 크롤링에 성공·실패 확인 장치가 없음|구성을 단순하게 가져감|작업 끝에 결과 건수를 지표로 남기고 0건이면 알림|
|CSV 적재 에러를 하나씩 대응|처음 형식을 CSV로 정함|DB 추출 경로는 처음부터 Parquet|
|`PARTIALLY_LOADED`를 놓치기 쉬움|일부 행만 실패하면 파일 적재 자체는 끝난 것처럼 보임|적재 결과의 에러 행 수를 확인하고 0이 아니면 실패 처리|
|같은 파일명 재업로드 누락|파일을 덮어쓰는 업로드 습관을 고려하지 않음|업로드 규칙(파일명에 시점·고유 값)을 먼저 정하고 적재 방식을 고름|
|SQS 메시지가 숨겨져 처리 지연|여러 건 수신 후 일부만 처리|소비자를 분리하고 DLQ와 큐 지표 알람을 처음부터 둠|

---

## 9. 실무 체크리스트

- [ ] S3에 raw와 validated 영역을 나눠, 원본에서 다시 처리할 수 있는가.
- [ ] DB 추출 파일을 CSV가 아니라 Parquet으로 내리고 있는가.
- [ ] `MATCH_BY_COLUMN_NAME`을 쓴다면 대상 DDL과 추출 컬럼 목록이 한 정의에서 나오는가.
- [ ] 개인정보 컬럼을 적재 테이블이 아니라 추출 SELECT에서 빼고 있는가.
- [ ] 증분 적재의 추출 조건과 삭제 조건이 같은 식인가.
- [ ] S3 이벤트를 `ObjectCreated:*`로 걸어 멀티파트 업로드도 받는가.
- [ ] 15분이 넘을 수 있는 작업을 Lambda에 두지 않았는가.
- [ ] EventBridge cron의 요일 번호(1=일요일)와 시간대를 확인했는가.
- [ ] SQS에서 받은 메시지를 모든 경로에서 지우거나, DLQ로 보내는가.
- [ ] Snowpipe에 같은 파일명이 다시 올라올 수 있는지 확인했는가.
- [ ] "작업 성공"과 별개로 결과 건수를 확인하는가.

---

## 10. 정리

- **S3를 경계로 둔다.** 원천과 적재 대상 사이를 파일로 끊어 두면 재처리, 추적, 형식 검증을 한곳에서 할 수 있다.
- **실행 환경은 작업 길이와 형태로 고른다.** 짧은 파일 변환은 Lambda, 브라우저가 필요한 긴 작업은 EC2, Spark 정제는 Glue나 EMR Serverless.
- **서버 없이 조회까지.** Crawler, Catalog, Athena로 수집 결과를 SQL로 볼 수 있게 했다.
- **비슷해 보이는 설정이 가장 위험하다.** cron 요일 번호, S3 이벤트 종류, Snowpipe 파일명 규칙 모두 에러 없이 조용히 어긋났다.
- **SQS는 "받았다"와 "지웠다"를 구분해야 한다.** 받은 수와 지운 수의 차이가 곧 문제 구간이다.

---

## References

설정과 동작은 각 서비스의 공식 문서를 기준으로 했고, 에러 사례와 설계 판단은 작업 중 직접 기록한 일지를 바탕으로 했다. 버킷·큐·테이블 이름은 예시 값으로 바꿨다.

- AWS — [Amazon S3 Event Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html)
- AWS — [Lambda quotas](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html)
- AWS — [AWS Glue: Working with crawlers](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html)
- AWS — [Monitoring AWS Glue jobs (DPU capacity)](https://docs.aws.amazon.com/glue/latest/dg/monitor-debug-capacity.html)
- AWS — [Amazon Athena: Partitioning data](https://docs.aws.amazon.com/athena/latest/ug/partitions.html)
- AWS — [AWS Step Functions Developer Guide](https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html)
- AWS — [EventBridge Scheduler: Schedule types (cron expressions)](https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html)
- AWS — [Amazon SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- AWS — [Using dead-letter queues in Amazon SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- AWS — [Available CloudWatch metrics for Amazon SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-available-cloudwatch-metrics.html)
- Snowflake — [Automating Snowpipe for Amazon S3](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3)
- Snowflake — [COPY INTO &lt;table&gt;](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
- Snowflake — [Load metadata (Snowpipe)](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro#load-history)
