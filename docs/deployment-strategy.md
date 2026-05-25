보통 이렇게 갑니다

  1. 앱 코드 수정
  2. Docker image 빌드
  3. Registry push
  4. Kubernetes manifest/Helm values 업데이트
  5. 배포 적용
  6. 스트리밍 job 상태 확인
  7. checkpoint / lag / latency 검증

  이때 배포 방식은 크게 3가지가 많습니다.

  1. Helm + kubectl 수동 배포
  가장 단순합니다.

  git push
  -> CI에서 image build/push
  -> 사람이 helm upgrade 또는 kubectl apply

  실시간 파이프라인 초반에는 이 방식이 현실적입니다. 배포가 잦지 않고, 장애와 상태를 눈으로 확인하기 좋습니다.

  2. CI/CD + Helm
  실무에서 가장 흔한 편입니다.

  git push
  -> CI가 test/build
  -> image push
  -> CI가 helm upgrade 실행

  여기서 중요한 건, 스트리밍 job은 배포 후 상태 검증이 필수라는 점입니다. 웹앱처럼 “떴다”로 끝나지 않습니다.

  배포 후 확인:

  - Kafka consumer lag
  - Flink checkpoint success
  - processing latency
  - duplicate rate
  - DLQ rate

  3. GitOps with Argo CD
  이건 가능하지만, 실시간 파이프라인에서는 보통 앱 배포 자체보다 인프라/설정 관리에 더 잘 맞습니다.

  Git commit
  -> Argo CD sync
  -> Kubernetes 반영

  Argo CD가 특히 좋은 것:

  - namespace
  - ConfigMap
  - Secret 템플릿
  - Helm values
  - Kafka/MinIO/Trino 같은 인프라 성격 리소스

  그런데 실시간 파이프라인은 job 상태와 데이터 정합성이 중요해서, Argo CD만으로 끝나지 않습니다. 배포 후에 다음
  을 별도로 봐야 합니다.

  - job restart 후 checkpoint 복구가 되는가
  - 새 image가 정상적으로 처리하는가
  - lag가 쌓이지 않는가
  - raw event와 processed event가 맞는가

  Flink 같은 스트리밍 job은 특히 다름

  Flink job은 단순 Deployment보다 운영 방식이 더 민감합니다.

  보통은:

  - image version 변경
  - savepoint 생성
  - 새 job 배포
  - savepoint로 복구
  - 이전 job 종료

  즉, 그냥 롤링업데이트보다 state-aware deployment에 가깝습니다.

  Kafka도 마찬가지입니다.

  - broker 설정 변경
  - topic config 변경
  - replication / ISR 확인
  - lag 및 produce/consume 상태 확인

  네 프로젝트에 맞는 추천

  너의 SkyLake는 이렇게 가면 좋습니다.

  인프라:
  - Git + Helm + kubectl
  - 나중에 Argo CD 도입 가능

  앱:
  - CI에서 Docker image 빌드
  - registry push
  - Flink job은 savepoint 기반 배포
  - Producer는 일반 Deployment 배포

  즉, Argo CD는 나중에 붙일 수 있지만, 실시간 파이프라인의 핵심은 배포 자동화보다 상태 보존과 복구 절차입니다.

  한 줄로 정리하면

  > 실시간 파이프라인은 단순히 “배포”가 아니라, 이미지 배포 후 checkpoint, lag, latency, data correctness까지 확
  > 인하는 운영 절차가 같이 들어가며, Argo CD는 그 중 인프라와 선언적 설정을 관리하는 데 잘 맞습니다.

# 배포 전략

## 개요

SkyLake는 일반적인 웹 애플리케이션보다 배포할 때 확인해야 할 항목이 더 많다. 단순히 컨테이너가 떠 있는지 보는 것이 아니라, 스트리밍 처리 상태와 데이터 정합성까지 함께 확인해야 하기 때문이다.

이 프로젝트의 배포는 다음 순서로 진행한다.

```text
1. 앱 코드 수정
2. Docker image 빌드
3. Registry push
4. Kubernetes manifest 또는 Helm values 업데이트
5. 배포 적용
6. 스트리밍 job 상태 확인
7. checkpoint / lag / latency / correctness 검증
```

## 기본 원칙

- 원본은 Git에 둔다.
- 서버는 실행 환경으로만 사용한다.
- 앱 코드는 이미지로 배포한다.
- 인프라 설정은 YAML, Helm values, playbook으로 관리한다.
- 스트리밍 job은 배포 후 상태 검증이 필수다.

## 배포 단위

### Producer

Producer는 일반적인 Kubernetes Deployment로 배포한다.

배포 방식:

```text
source code
  -> image build
  -> registry push
  -> Deployment rollout
```

확인 항목:

- Jetstream WebSocket 연결
- Kafka topic write 성공 여부
- reconnect 동작
- ingestion rate

### Kafka

Kafka는 StatefulSet 또는 Strimzi CRD로 배포한다.

배포 방식:

```text
Kafka operator
  -> Kafka CR
  -> Kafka topic CR
  -> broker reconciliation
```

확인 항목:

- broker readiness
- topic 생성
- retention 설정
- consumer lag

### Flink

Flink는 배포할 때 상태 보존을 고려해야 한다.

배포 방식:

```text
job code
  -> image build
  -> registry push
  -> savepoint 생성 여부 확인
  -> 새 job 제출
  -> checkpoint recovery 확인
```

확인 항목:

- job restart 여부
- checkpoint 성공 여부
- state recovery
- processing latency
- duplicate rate
- DLQ rate

### MinIO

MinIO는 object storage이므로 스토리지와 PVC 구성이 중요하다.

배포 방식:

```text
Deployment or StatefulSet
  -> PVC
  -> bucket bootstrap
```

확인 항목:

- bucket 생성
- warehouse 접근 가능 여부
- checkpoint 저장 가능 여부

### Iceberg REST Catalog

Iceberg REST Catalog는 metadata backend와 저장소 endpoint가 함께 맞아야 한다.

배포 방식:

```text
REST catalog deployment
  -> PostgreSQL backend
  -> MinIO warehouse endpoint
```

확인 항목:

- catalog 연결
- table create/read
- Flink/Trino 동시 접근

### Trino

Trino는 SQL query engine으로 배포한다.

배포 방식:

```text
config map
  -> deployment
  -> Iceberg catalog 연결
```

확인 항목:

- Iceberg table query
- row count
- profiling query latency

## Argo CD와의 관계

Argo CD는 이 프로젝트에서 선택 사항이다. 반드시 초기에 써야 하는 도구는 아니다.

Argo CD가 잘 맞는 영역:

- namespace
- ConfigMap
- Secret template
- Helm values
- 표준화된 인프라 리소스

Argo CD만으로는 충분하지 않은 영역:

- Flink job 상태 복구
- Kafka topic 운영 상태
- checkpoint와 savepoint 검증
- data correctness 검증

즉, Argo CD는 선언적 배포와 동기화에 좋지만, 실시간 파이프라인은 배포 후 검증 절차가 별도로 필요하다.

## 추천 흐름

### Phase 1

초기에는 단순한 방식으로 시작한다.

```text
Mac에서 수정
  -> Git commit/push
  -> 서버 또는 로컬에서 kubectl apply / helm upgrade
  -> 상태 확인
```

### Phase 2

반복 배포가 많아지면 Helm과 CI를 추가한다.

```text
Git push
  -> CI build/test
  -> image push
  -> helm upgrade
  -> post-deploy verification
```

### Phase 3

인프라와 설정이 안정되면 Argo CD를 붙인다.

```text
Git push
  -> Argo CD sync
  -> Kubernetes reflect changes
```

## 실시간 파이프라인 특성

실시간 파이프라인은 일반 앱처럼 "배포 완료"만으로 끝나지 않는다.

배포 후 반드시 확인해야 할 것:

- consumer lag
- processing latency
- checkpoint duration
- event duplication
- late event rate
- DLQ rate
- raw count와 processed count의 일치성

## 결론

SkyLake의 배포는 "이미지 배포"와 "상태 검증"을 함께 다뤄야 한다. Argo CD는 선언적 설정과 인프라 동기화에 유용하지만, 스트리밍 job의 운영 상태는 별도의 검증 절차가 필요하다. 따라서 초기에는 Git + Helm + kubectl 기반으로 시작하고, 배포가 반복되며 복잡해질 때 Argo CD를 도입하는 것이 가장 현실적이다.
