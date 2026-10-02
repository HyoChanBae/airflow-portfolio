# Snowflake → Supabase 마이그레이션

`dags/snowflake_to_supabase.py`는 Snowflake 테이블을 Supabase의 PostgreSQL로
배치 복사하는 수동 실행 Airflow DAG입니다. 데이터는 XCom이나 로컬 파일에 저장하지
않고 `fetchmany`로 읽어서 PostgreSQL bulk insert로 전달합니다.

## 1. Airflow Connection

기존 프로젝트에서 사용하는 다음 Connection이 필요합니다.

- `snowflake_conn3`: Snowflake 연결
- `supabase_postgres`: Supabase PostgreSQL 직접 연결 또는 session pooler 연결

Supabase 연결은 SSL을 사용하도록 Connection의 Extra에 아래 값을 설정합니다.

```json
{"sslmode": "require"}
```

## 2. 테이블 설정

Admin → Variables에서 `snowflake_to_supabase_tables`라는 JSON Variable을 만들고
아래처럼 설정합니다.

```json
[
  {
    "source": "DEMO_RAW_DB.RAW.CUSTOMERS",
    "target": "public.customers",
    "write_mode": "replace",
    "batch_size": 5000,
    "lowercase_columns": true
  }
]
```

Airflow CLI를 사용한다면 다음과 같이 등록할 수 있습니다.

```bash
airflow variables set snowflake_to_supabase_tables '[{"source":"DEMO_RAW_DB.RAW.CUSTOMERS","target":"public.customers","write_mode":"replace","batch_size":5000,"lowercase_columns":true}]' --json
```

DAG 실행 시 JSON conf의 `tables`에 같은 배열을 전달하면 Variable보다 우선합니다.

```json
{
  "tables": [
    {
      "source": "DEMO_RAW_DB.RAW.CUSTOMERS",
      "target": "public.customers",
      "write_mode": "replace",
      "batch_size": 5000,
      "lowercase_columns": true
    }
  ]
}
```

## 적재 모드

- `replace`: 대상 테이블을 삭제한 뒤 Snowflake 스키마로 다시 생성합니다.
- `truncate`: 대상 테이블을 유지하면서 모든 행을 지운 후 다시 적재합니다.
- `append`: 기존 행을 유지하고 새 행을 추가합니다.
- `upsert`: `primary_key_columns`를 기준으로 insert/update합니다. 대상 테이블에 해당
  PK 또는 UNIQUE 제약조건이 있어야 합니다.

Upsert 설정 예시:

```json
{
  "source": "DEMO_RAW_DB.RAW.CUSTOMERS",
  "target": "public.customers",
  "write_mode": "upsert",
  "primary_key_columns": ["CUSTOMER_ID"],
  "batch_size": 5000,
  "lowercase_columns": true
}
```

일반 Snowflake 객체명은 대문자로 조회합니다. 따옴표로 생성한 대소문자 구분 객체를
옮길 때만 `"source_case_sensitive": true`를 추가합니다.

## 주의사항

- `replace`는 대상 테이블과 그 데이터, 인덱스, 권한 정책을 새로 만듭니다.
  Supabase RLS나 별도 인덱스가 있는 운영 테이블은 `truncate` 또는 `upsert`를
  사용하세요.
- 여러 테이블을 설정하면 테이블 단위로 commit됩니다. 실패한 테이블의 변경은
  rollback되고 이미 완료된 이전 테이블은 유지됩니다.
- `append` 실행을 수동으로 다시 시작하면 이미 완료된 테이블의 행이 중복될 수
  있습니다. 재실행 가능성이 있으면 PK를 지정한 `upsert`를 사용하세요. 이 문제를
  피하기 위해 DAG 태스크의 자동 retry는 비활성화되어 있습니다.
- `replace`/`truncate`는 적재 후 행 수를 검증합니다.
