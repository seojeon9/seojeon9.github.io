---
title: "검색 시스템 만들기 1: 흩어진 원천 데이터를 검색용 마트로 (PySpark on EMR Serverless)"
excerpt: "자연어로 후보를 찾는 검색 시스템의 첫 단계로, 여러 시스템에 흩어진 프로필·이력·평가 데이터를 Spark로 모아 검색용 마트를 만든 기록이다. Spark와 EMR Serverless를 고른 이유와 그 대가, Airflow에서 작업을 제출하는 방법, 테이블이 늘어도 코드를 고치지 않는 설정 기반 적재, 임베딩·LLM 분류 API를 executor에 나눠 부른 방법과 LLM 출력 검증, 원천 데이터의 숨은 필터 때문에 생긴 문제를 정리했다."
categories: [spark]
tags: [spark, pyspark, emr-serverless, airflow, etl, data-mart, postgresql, 포트폴리오]
date: 2026-11-20 09:00:00 +0900
last_modified_at: 2026-11-20
---

## 이 글을 30초에 파악하기

> **한 줄로** — 검색할 데이터가 여러 시스템에 흩어져 있어서, Spark로 원천을 조인·정제하고 임베딩까지 붙여 검색용 마트 하나로 모으는 배치 파이프라인을 만든 기록이다.

**이 글에서 보여주는 것**

- 데이터 양이 크지 않은데도 Spark를 택한 이유, 그리고 솔직한 대가
- EC2 / EMR 클러스터 / EMR Serverless 비교와 선택
- Airflow에서 EMR Serverless로 PySpark 작업을 제출하는 코드와 손이 많이 간 설정(Python 패키지, 로그)
- 테이블이 늘어도 Spark 코드를 고치지 않도록 만든 설정 기반 적재(INSERT / UPSERT)
- 임베딩·LLM 분류 같은 외부 API 호출을 executor에 나눠 병렬로 돌린 방법
- LLM이 깨진 JSON을 돌려줄 때 배치가 멈추지 않게 만든 검증·재시도
- 원천 테이블의 숨은 필터 조건 때문에 생긴 데이터 누락과 해결

**용어 먼저 보기**

|용어|한 줄 설명|이 글에서의 역할|
|---|---|---|
|데이터 마트|특정 용도에 맞게 가공해 둔 테이블 묶음|검색 API가 바로 읽는 테이블|
|PySpark|Spark를 Python으로 쓰는 API|조인·정제·임베딩 배치|
|pandas UDF|Spark 컬럼을 pandas Series 단위로 받아 처리하는 사용자 함수|executor별 임베딩·분류 API 호출|
|broadcast|작은 데이터를 모든 executor에 한 번만 보내 두는 Spark 기능|BM25 모델 배포|
|EMR Serverless|클러스터 없이 Spark 작업 단위로 자원을 쓰는 AWS 서비스|ETL 실행 환경|
|UPSERT|키가 있으면 갱신, 없으면 삽입. PostgreSQL에서는 `ON CONFLICT DO UPDATE`|자주 바뀌는 테이블 적재|
|JDBC|Java 계열에서 DB에 접속하는 표준 인터페이스|Spark 기본 쓰기 방식. UPSERT를 못 해서 직접 쓰기로 대체(4.2)|

**프로젝트 개요**

|항목|내용|
|---|---|
|기간|2024.07 ~ 2025.02 (8개월)|
|역할|데이터 엔지니어: 데이터 파이프라인 설계·구축, 유사도 검색 로직 구현|
|대상 데이터|수천 명 규모의 사내 프로필, 이력, 평가·경력 서술 텍스트|
|이 글의 스택|PySpark, AWS EMR Serverless, Apache Airflow, Amazon S3, PostgreSQL 16 + pgvector (RDS), OpenAI API, Kiwi·BM25|

---

이 시리즈는 사내에 쌓인 프로필·평가 데이터에서 **자연어 한 줄로 조건에 맞는 후보를 찾는 검색 시스템**을 만든 프로젝트를 두 편으로 나눠 정리한 기록이다.

- **1편 (이 글)**: 데이터 파이프라인. 원천 DB에서 검색용 마트까지
- **[2편](/rag/hybrid-search-pgvector-bm25/)**: 하이브리드 서치. Dense 임베딩과 BM25를 섞고, LLM으로 질의에서 조건을 꺼내는 검색 로직

RAG로 치면 이 프로젝트는 R(검색)을 제대로 만드는 일이었고, 그 R이 읽을 데이터를 만드는 것이 이 글이다.

---

## 1. 배경: 검색할 데이터가 한곳에 없었다

후보를 찾는 일은 원천 시스템을 조회해 엑셀로 내려받고, 부서·직급으로 필터한 뒤 경험으로 추리는 방식이었다. 검색 시스템을 만들려고 보니 첫 문제는 알고리즘이 아니라 **데이터 위치**였다.

|데이터|있던 곳|검색에서의 쓰임|
|---|---|---|
|기본 프로필 (부서, 직급, 직무)|인사 원천 시스템|정형 필터|
|조직·직무 이력|인사 원천 시스템의 이력 테이블|경험·경력 필터|
|평가 서술 텍스트|평가 시스템|역량·성향 유사도 검색|
|경력 서술 텍스트|별도 경력 관리 데이터|경험 유사도 검색|

목표는 이 데이터를 **검색 API가 조인 없이 바로 읽을 수 있는 마트**로 모으고, 텍스트 컬럼에는 임베딩 벡터까지 미리 붙여 두는 것이었다.

---

## 2. 설계 결정

### 2.1 왜 Spark였나

솔직히 초기 데이터 양만 보면 Spark가 꼭 필요한 규모는 아니었다. 그래도 Spark를 택한 이유는 이랬다.

|고려 사항|판단|
|---|---|
|데이터 증가|이력성 데이터라 계속 쌓이고, 특정 시즌에 한꺼번에 몰리는 데이터도 있었다|
|원천과의 분리|원천 시스템에 가공 부하를 주지 않고, 분리된 환경에서 조인·정제를 끝내야 했다|
|한 흐름으로 처리|정제 → 임베딩 → 적재를 하나의 작업으로 묶고 싶었다|
|팀 역량|팀에서 이미 PySpark를 쓰고 있었다|
|외부 API 호출|텍스트 한 건마다 임베딩·LLM 분류 API를 불러야 했다. 행 수는 적어도 호출은 느려서, executor에 나눠 병렬로 부르는 쪽이 효과가 컸다|

마지막 줄이 실제로 가장 체감이 컸다. 조인·정제만 보면 단일 서버로도 충분했지만, 수천 건의 텍스트에 API를 하나씩 부르는 단계는 파티션 수만큼 동시에 돌릴 수 있다는 점에서 Spark가 값을 했다.

### 2.2 실행 환경: 왜 EMR Serverless였나

|선택지|장점|단점|판단|
|---|---|---|---|
|EC2에 Spark 직접 설치|비용 예측이 쉬움|하루 한 번 도는 배치를 위해 서버가 계속 떠 있음, 버전·패키지 관리 부담|보류|
|EMR 클러스터|Spark 운영이 검증됨|클러스터를 켜고 끄는 관리가 필요|보류|
|**EMR Serverless**|작업 단위로 자원을 쓰고 끝나면 반납, 클러스터 관리 없음|작업마다 기동 시간, 모든 설정을 submit 파라미터로 넘겨야 함|**선택**|

전날 데이터를 새벽에 처리하는 배치라, 실패해도 업무 시간 전에 다시 돌릴 여유가 있었다. 그래서 "일단 온디맨드로 돌려 보고, 비용이 보이면 최적화한다"로 시작했다.

### 2.3 전체 흐름

```
[원천 DB (사내 DW)]
        │ SQL 조회 (읽기 전용 계정, 방화벽으로 허용된 경로만)
        ▼
  EMR Serverless (PySpark) ◀── 작업 제출 ── Airflow
        │                                  trigger DAG → batch DAG
        │ 조인·정제·이력 통합
        │ 텍스트 → Dense·Sparse 벡터
        │ 평가 텍스트 → 유형 태그(LLM)
        ▼
  PostgreSQL 16 + pgvector (RDS)   ◀── 검색 API가 읽는 마트

  S3: Spark 코드, 테이블별 SQL, 적재 설정, BM25 모델, Python 환경, 로그
```

위 다이어그램은 실행에 필요한 것이 전부 S3에 있다는 점을 보여준다. EMR Serverless는 매번 새 환경에서 뜨기 때문에, 코드·설정·패키지·모델을 모두 S3 경로로 넘겨야 한다. 반대로 말하면 S3만 맞춰 두면 어디서 제출해도 같은 작업이 돈다.

---

## 3. Airflow에서 Spark 작업 제출하기

### 3.1 DAG 구성

`trigger DAG`가 배치 주기(일·주·월 등)를 정해 `batch DAG`를 부르고, `batch DAG`는 그 주기 값을 Spark 설정으로 넘겨 작업을 제출한다. 어떤 테이블을 돌릴지는 Spark 쪽이 주기 값을 보고 설정 파일에서 고른다(4절).

```python
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

run_etl = EmrServerlessStartJobOperator(
    task_id="source_to_mart",
    application_id="<EMR_APP_ID>",
    execution_role_arn="<JOB_ROLE_ARN>",
    job_driver={
        "sparkSubmit": {
            "entryPoint": "s3://<BUCKET>/job_scripts/source_to_mart.py",
            "sparkSubmitParameters": " ".join([
                "--conf spark.batch.config={% raw %}{{ params.cycle }}{% endraw %}",   # daily / weekly / monthly
                "--conf spark.archives=s3://<BUCKET>/python_env/pyspark_env.tar.gz#environment",
                "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python",
                "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python",
                "--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python",
                "--conf spark.dynamicAllocation.enabled=true",
                "--conf spark.dynamicAllocation.minExecutors=4",
                "--conf spark.dynamicAllocation.maxExecutors=20",
            ]),
        }
    },
    configuration_overrides={
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": "s3://<BUCKET>/spark_logs/"}
        }
    },
)
```

### 3.2 손이 많이 간 설정

EMR Serverless는 작업마다 깨끗한 환경에서 뜨기 때문에, 서버에 한 번 설치해 두면 끝나던 것들을 매 작업에 같이 넘겨야 한다.

|필요한 것|이유|넘기는 방법|
|---|---|---|
|Python 패키지 (OpenAI SDK, LangChain, Kiwi, BM25 인코더, psycopg2 등)|기본 런타임에 없음|가상환경을 통째로 압축해 S3에 올리고 `spark.archives`로 첨부|
|그 가상환경의 Python을 쓰라는 지정|driver와 executor의 Python 경로 설정이 따로 있음|`driverEnv.PYSPARK_PYTHON`, `executorEnv.PYSPARK_PYTHON`을 둘 다 지정|
|executor에서 쓸 환경 변수|임베딩 API는 executor에서 호출됨. driver에만 있는 값은 executor에 안 보임|`spark.executorEnv.*`로 executor에도 전달|
|로그 보관 위치|작업이 끝나면 환경이 사라짐|`logUri`로 driver·executor 로그를 S3에 남김|

가상환경은 EMR Serverless 런타임과 같은 OS·Python 버전에서 만들어야 한다. 네이티브 확장이 있는 패키지는 다른 OS에서 만든 환경으로는 동작하지 않는다.

---

## 4. 설정 기반 적재

### 4.1 왜 설정으로 뺐나

마트 테이블은 처음 몇 개에서 시작해 금방 열 개 가까이로 늘었다. 테이블마다 Spark 스크립트를 만들면 적재 로직이 조금씩 달라지고, 고칠 곳이 늘어난다. 그래서 **Spark 코드는 하나**로 두고, 테이블별 차이는 S3에 둔 파일 세 종류로 표현했다.

|파일|담는 것|
|---|---|
|`sql/<테이블>.sql`|원천에서 이 테이블을 만드는 조회 SQL. 파일 이름이 곧 대상 테이블 이름|
|`batch_config.json`|주기별로 돌릴 테이블 목록|
|`table_config.json`|테이블별 적재 방식, 유니크 키, 임베딩·분류 대상 컬럼|

```json
// batch_config.json — 주기 값 하나로 대상 테이블이 정해진다
{
  "daily":      ["MART_PROFILE"],
  "weekly":     ["MART_HISTORY"],
  "monthly":    ["MART_EXPERIENCE", "MART_TALENT_TYPE"]
}
```

```json
// table_config.json
{
  "MART_PROFILE":     { "load_type": "upsert", "unique_keys": ["EMPNO"],
                        "embedding_key": null,      "classify_key": null },
  "MART_EXPERIENCE":  { "load_type": "insert", "unique_keys": [],
                        "embedding_key": "EXP_TEXT", "classify_key": null },
  "MART_TALENT_TYPE": { "load_type": "insert", "unique_keys": [],
                        "embedding_key": null,      "classify_key": "EVAL_TEXT" }
}
```

Spark 진입 스크립트는 이 설정을 읽어 테이블마다 같은 순서를 반복한다.

```python
cycle = spark.conf.get("spark.batch.config", "weekly")
tables = batch_config.get(cycle, [])

for sql_path in [p for p in list_s3_sql_files(SQL_FOLDER) if table_of(p) in tables]:
    cfg = table_config[table_of(sql_path)]
    sdf = load_source(read_s3(sql_path))               # 원천 조회 → Spark DataFrame

    if cfg["embedding_key"]:                           # 4.3
        sdf = add_vectors(sdf, cfg["embedding_key"])
    elif cfg["classify_key"]:                          # 4.4
        sdf = add_talent_type(sdf, cfg["classify_key"])

    load(sdf, cfg["load_type"], cfg["unique_keys"])    # 4.2
```

새 테이블이 필요하면 SQL 파일 하나와 설정 두 줄을 추가하면 끝났다. Spark 코드는 손대지 않았다.

### 4.2 적재 방식 고르는 기준

|테이블 성격|적재 방식|이유|
|---|---|---|
|프로필처럼 대상별로 한 행, 자주 일부만 바뀜|UPSERT|바뀐 대상만 갱신하면 되고, 검색 중인 테이블을 비우지 않아도 됨|
|평가처럼 주기마다 통째로 새로 생김|INSERT (전체 재적재)|과거 행을 맞춰 갱신하는 것보다 비우고 다시 넣는 게 단순하고 정확함|
|이력처럼 과거 기록이 소급 수정될 수 있음|INSERT (전체 재적재)|어떤 행이 바뀌었는지 추적하는 비용이 더 큼|

PostgreSQL의 UPSERT는 `INSERT ... ON CONFLICT (키) DO UPDATE`다. Spark의 기본 JDBC 쓰기는 이 구문을 만들지 못한다. 그래서 쓰기는 Spark writer 대신 **각 파티션이 직접 DB에 연결해 묶음으로 넣는** 방식으로 만들었다.

```python
def upsert_partition(partition, columns, table, keys, batch_size=5000):
    conn = connect_target_db()
    cur = conn.cursor()
    placeholders = ", ".join("%s::vector" if c == "dense_vector" else "%s" for c in columns)
    sql = f"""
        INSERT INTO {table} ({", ".join(columns)}) VALUES ({placeholders})
        ON CONFLICT ({", ".join(keys)}) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in keys)}
    """
    buf = []
    for row in partition:
        buf.append(tuple(getattr(row, c) for c in columns))
        if len(buf) >= batch_size:
            cur.executemany(sql, buf); conn.commit(); buf.clear()
    if buf:
        cur.executemany(sql, buf); conn.commit()
    conn.close()

sdf.rdd.foreachPartition(lambda p: upsert_partition(p, sdf.columns, table, keys))
```

|포인트|이유|
|---|---|
|연결을 파티션 안에서 연다|DB 연결 객체는 직렬화해 executor로 보낼 수 없다. 파티션마다 열고 닫는다|
|5,000건씩 `executemany` + commit|한 건씩 넣으면 왕복이 너무 많고, 전부 모아 넣으면 메모리와 트랜잭션이 커진다|
|`%s::vector` 캐스팅|Python 리스트로 넘긴 Dense 벡터를 pgvector 타입으로 바꿔 넣는다|
|INSERT 방식은 먼저 `TRUNCATE`|driver에서 대상 테이블을 한 번 비운 뒤, 파티션들이 같은 구조에서 `ON CONFLICT`만 뺀 INSERT를 실행|

### 4.3 텍스트 컬럼: API 호출을 executor에 나누기

`embedding_key`가 있는 테이블은 같은 작업 안에서 벡터까지 만든다. Dense는 OpenAI `text-embedding-ada-002`, Sparse는 Kiwi 형태소 분석 + BM25다. 둘 다 pandas UDF로 만들어 executor에서 돌렸다.

```python
@pandas_udf(ArrayType(FloatType()))
def dense_udf(texts: pd.Series) -> pd.Series:
    client = OpenAI()                         # executor 안에서 생성
    out = []
    for t in texts:
        try:
            out.append(client.embeddings.create(model=MODEL, input=t).data[0].embedding)
            time.sleep(1)                     # 요청 한도 대응
        except Exception as e:
            print(e); out.append(None)
    return pd.Series(out)

bm25_model = sc.broadcast(load_json_from_s3("models/bm25_encoder.json"))

sdf = (sdf.repartition(200)
          .withColumn("dense_vector",  dense_udf(col(key)))
          .withColumn("sparse_vector", sparse_udf(bm25_model.value)(col(key))))
```

|결정|이유|
|---|---|
|`repartition(200)`|원천 조회 직후에는 파티션이 적다. 잘게 나눠야 API 호출이 여러 executor로 퍼진다|
|BM25 모델을 `broadcast`|문서 집합으로 미리 학습한 BM25 파라미터(JSON)를 executor마다 S3에서 다시 읽지 않게 한 번만 배포|
|Sparse 벡터는 `{"토큰 인덱스": 가중치}` JSON 문자열로 저장|0이 아닌 값만 남겨 저장 공간을 줄이고, 검색 함수에서 그대로 읽음|
|`time.sleep`으로 요청 간격 조절|파티션이 동시에 돌면 API 요청 한도에 바로 걸린다|

벡터를 어떻게 쓰는지는 [2편](/rag/hybrid-search-pgvector-bm25/)에서 다룬다. 검색 시점에는 질의 하나만 임베딩하면 되도록, 무거운 임베딩·분류는 전부 이 배치에서 끝낸다.

### 4.4 LLM 분류 결과를 믿지 않기

`classify_key`가 있는 테이블은 평가 텍스트를 GPT-4o-mini로 미리 정한 11가지 인재 유형으로 분류한다. 결과는 요약 한 줄과 유형별 해시태그·근거 문장을 담은 중첩 JSON이다. 배치에서 LLM 출력을 받으면 **형식이 깨질 수 있다**는 전제로 세 겹을 뒀다.

|단계|하는 일|
|---|---|
|1. 고쳐서 파싱|작은따옴표를 큰따옴표로 바꾸고, 자주 나오는 괄호 중복 패턴을 고친 뒤 `json.loads`. 안 되면 `ast.literal_eval`로 한 번 더|
|2. 재시도|그래도 JSON이 아니면 최대 5번 다시 호출|
|3. 값 검증|돌아온 유형이 정해진 11개 목록에 없으면 버린다. 남는 게 없으면 "분석 데이터 부족" 기본값으로 채운다|

마지막 단계가 중요했다. 형식은 맞는데 목록에 없는 유형을 만들어 내는 경우가 있었고, 그대로 적재하면 화면의 유형 필터가 깨졌다. 검증을 통과한 JSON은 Spark에서 `from_json` + `explode`로 펼쳐 사람 × 유형 × 해시태그 단위의 행으로 적재했다.

```python
(sdf.withColumn("answer", classify_udf(col(key)))
    .withColumn("t",   explode(from_json(get_json_object("answer", "$.types"), SCHEMA)))
    .withColumn("tag", explode(col("t.tags")))
    .select("empno", "t.type", "tag.hashtag", "tag.evidence"))
```

이 JSON 처리는 운영에 들어간 뒤에도 한 번 더 고쳤다. 새로 나온 깨진 패턴 하나 때문에 분류 배치가 실패했고, 1단계 치환 규칙을 추가해 해결했다.

---

## 5. 원천의 숨은 필터

마트를 처음 채웠을 때 에러 없이 끝났는데, 건수가 기대보다 적었다. 원천 테이블은 원래 용도에 맞게 **특정 상태의 행만 담도록 이미 걸러져 있었고**, 그 조건은 테이블 이름이나 문서 어디에도 드러나 있지 않았다. 상태별로 나뉘어 있던 원천을 합치고 상태 컬럼으로 구분해 해결했다.

배치는 성공했고 스키마도 맞았기 때문에, 건수를 직접 대조하기 전까지는 아무 신호도 없었다. **원천 테이블의 숨은 필터 조건**을 확인하지 않으면 마트가 조용히 일부만 담는다. 이후로는 원천을 붙이기 전에 원천 건수와 기대 건수를 먼저 맞춰 봤다.

---

## 6. 회고: 다시 한다면

|한계|왜 그렇게 됐나|다시 한다면|
|---|---|---|
|데이터 양 대비 무거운 Spark|증가를 대비한 선택|초기에는 가벼운 배치(단일 컨테이너 + pandas/polars)로 시작하고, 증가가 확인되면 Spark로 전환|
|작업마다 기동 시간|EMR Serverless 특성|자주 도는 작은 테이블은 묶어서 한 작업으로 제출|
|원천의 숨은 필터를 늦게 발견|테이블 구조만 보고 마트를 설계|EDA 단계에서 원천 건수와 "기대 건수"를 먼저 대조|
|임베딩 비용이 전체 재적재마다 발생|INSERT 테이블은 매번 전부 다시 임베딩|텍스트 해시를 저장해 바뀐 텍스트만 다시 임베딩|
|실패가 조용히 묻힘|API 실패는 `None`으로, 파티션 적재 실패는 로그 출력만 하고 넘어감|실패 건수를 집계해 임계치를 넘으면 작업 실패로 처리하고, 적재 후 원천 건수와 대조|
|전체 재적재 중 테이블이 빔|`TRUNCATE` 후 적재가 끝날 때까지 검색이 빈 테이블을 봄|새 테이블에 적재한 뒤 이름을 바꿔 교체하거나, 한 트랜잭션 안에서 교체|
|원천 조회가 driver 한 곳에서 일어남|psycopg2로 조회해 pandas → Spark DataFrame으로 변환. 수천 건이라 충분했음|데이터가 커지면 Spark JDBC 읽기를 파티션 컬럼 기준으로 병렬화|
|요청 한도를 `sleep`으로 맞춤|구현이 가장 단순했음|한도 초과 응답에만 지수 백오프로 재시도하고, 임베딩은 여러 건을 한 요청에 묶어 보냄|

---

## 7. 실무 체크리스트

- [ ] 데이터 양이 Spark를 정당화하는가, 아니면 증가 가능성만으로 고른 것인가.
- [ ] EMR Serverless에 필요한 Python 패키지를 런타임과 같은 OS·Python 버전으로 묶었는가.
- [ ] driver와 executor의 Python 경로를 둘 다 지정했는가.
- [ ] executor에서 필요한 환경 변수를 executor에도 넘기고 있는가.
- [ ] Spark 로그가 작업 종료 후에도 S3에 남는가.
- [ ] 테이블별 적재 방식·키·주기가 코드가 아니라 설정에 있는가.
- [ ] UPSERT 테이블에 실제 유니크 제약이 걸려 있는가.
- [ ] 외부 API를 부르는 UDF 앞에서 파티션 수를 늘려 호출을 분산했는가.
- [ ] LLM 출력을 형식(JSON)과 값(허용 목록) 두 번 검증하는가.
- [ ] UDF가 `None`으로 넘긴 실패 건수를 작업 끝에 확인하는가.
- [ ] 원천 테이블에 숨은 필터(특정 상태만, 특정 기간만)가 없는지 건수로 확인했는가.

---

## 8. 정리

- **검색의 첫 문제는 데이터 위치였다.** 알고리즘보다 먼저, 흩어진 데이터를 조인 없이 읽을 수 있는 마트로 모았다.
- **실행 환경은 배치 성격에 맞췄다.** 하루 한 번 도는 배치라 상시 클러스터 대신 EMR Serverless를 택했다.
- **차이는 설정으로, 로직은 하나로.** 테이블이 늘어도 Spark 코드는 그대로 두고 SQL 파일과 설정만 추가했다.
- **Spark의 값은 API 병렬 호출에서 나왔다.** 데이터는 작았지만, 텍스트마다 부르는 임베딩·분류 API를 executor에 나눈 것이 체감이 컸다.
- **LLM 출력은 검증한 뒤에 적재했다.** 형식이 깨지거나 목록에 없는 값이 나와도 배치가 멈추거나 화면이 깨지지 않게 했다.
- **원천은 생각보다 많이 걸러져 있었다.** 숨은 필터를 건수로 확인하는 습관이 남았다.

[다음 편](/rag/hybrid-search-pgvector-bm25/)에서는 이 마트 위에서 Dense 임베딩과 BM25를 섞어 후보를 찾는 하이브리드 서치를 다룬다.

---

## References

설정 항목은 각 도구의 공식 문서를 기준으로 했고, 설계 판단과 에러 사례는 프로젝트 진행 중 직접 기록한 작업 일지를 바탕으로 했다. 사내 시스템명·테이블명·계정·버킷은 예시 값으로 바꿨다.

- AWS — [Amazon EMR Serverless User Guide](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html)
- AWS — [Using Python libraries with EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/using-python-libraries.html)
- AWS — [Spark job properties for EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-spark.html)
- Apache Airflow — [Amazon EMR Serverless Operators](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/emr/emr_serverless.html)
- Apache Spark — [Pandas UDFs (Vectorized UDFs)](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
- Apache Spark — [Broadcast Variables](https://spark.apache.org/docs/latest/rdd-programming-guide.html#broadcast-variables)
- Apache Spark — [JDBC To Other Databases](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
- PostgreSQL — [INSERT ... ON CONFLICT](https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT)
