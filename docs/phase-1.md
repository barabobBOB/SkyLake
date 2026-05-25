# Phase 1 실행 계획

## 목표

Phase 1의 목표는 Bluesky Jetstream 이벤트를 정제하지 않고 Iceberg Bronze 테이블에 안정적으로 저장하는 것이다.

```text
Bluesky Jetstream
  -> Bluesky Producer
  -> Kafka topic: bluesky.raw.v1
  -> Flink bronze-ingest job
  -> Iceberg table: bluesky.bronze_raw
```

이 단계에서는 Silver, Gold, deduplication, watermark, DLQ 처리를 구현하지 않는다. 먼저 raw 데이터를 충분히 쌓고, 그 데이터를 기반으로 후속 처리 정책을 결정한다.

## 결정 사항

- Broker는 Kafka로 진행한다.
- Kafka는 Phase 1에서 1 broker로 시작한다.
- 두 번째 노드를 먼저 Kubernetes cluster에 join한다.
- 초기 cluster는 2노드 역할 분리 구조로 운영한다.
- Kafka/Flink/Producer는 `node-1`에 배치한다.
- MinIO/Iceberg REST/PostgreSQL/Trino는 `node-2`에 배치한다.

## 2노드 배치

```text
node-1: lena-cloud
  역할: streaming + processing
  workload:
    - Kubernetes control-plane
    - existing Airflow
    - Kafka broker 1
    - Bluesky Producer
    - Flink Kubernetes Operator
    - Flink JobManager
    - Flink TaskManager

node-2: lakehouse-node
  역할: storage + query
  workload:
    - MinIO
    - Iceberg REST Catalog
    - Iceberg PostgreSQL
    - Trino
    - Grafana
```

## Step 1. 두 번째 노드 Join

현재 control-plane은 `lena-cloud`다.

두 번째 서버는 worker node로 join한다. control-plane HA는 3노드 이상에서 다시 고려한다.

### control-plane에서 join command 생성

`lena-cloud`에서 실행한다.

```bash
sudo kubeadm token create --print-join-command
```

출력 예시는 다음과 같다.

```bash
sudo kubeadm join 192.168.219.250:6443 \
  --token <token> \
  --discovery-token-ca-cert-hash sha256:<hash>
```

### 두 번째 노드에서 join command 실행

두 번째 서버에서 container runtime, kubelet, kubeadm 설치가 끝난 뒤 위 join command를 실행한다.

```bash
sudo kubeadm join 192.168.219.250:6443 \
  --token <token> \
  --discovery-token-ca-cert-hash sha256:<hash>
```

### join 확인

`lena-cloud`에서 확인한다.

```bash
kubectl get nodes -o wide
```

두 번째 노드가 `Ready` 상태가 되어야 한다.

## Step 2. 노드 라벨링

노드 이름을 확인한다.

```bash
kubectl get nodes
```

라벨을 부여한다.

```bash
kubectl label node lena-cloud node-pool=streaming-processing --overwrite
kubectl label node lena-cloud storage=local-nvme --overwrite

kubectl label node <second-node-name> node-pool=lakehouse-query --overwrite
kubectl label node <second-node-name> storage=local-disk --overwrite
```

라벨 확인:

```bash
kubectl get nodes --show-labels
```

## Step 3. Namespace 생성

```bash
kubectl apply -f infra/k8s/namespaces/namespaces.yaml
```

생성되는 namespace:

```text
streaming
processing
lakehouse
observability
```

## Step 4. Lakehouse 계층 배포

먼저 저장 계층을 배포한다.

배포 순서:

```text
1. MinIO
2. Iceberg PostgreSQL
3. Iceberg REST Catalog
4. Trino
```

MinIO bucket:

```text
iceberg-warehouse
flink-checkpoints
flink-savepoints
```

## Step 5. Streaming 계층 배포

Kafka는 Strimzi Kafka Operator를 사용해 배포한다.

Phase 1 Kafka 구성:

```text
broker: 1
topic: bluesky.raw.v1
partitions: 6
replication.factor: 1
retention: 1~3 days
```

3노드가 되기 전까지는 3 broker 구성을 억지로 만들지 않는다. 2노드에서 3 broker를 띄우면 실제 장애 도메인이 2개뿐이라 HA 효과가 제한적이고 리소스만 더 사용한다.

## Step 6. Processing 계층 배포

Flink Kubernetes Operator를 배포한 뒤 bronze ingest job을 배포한다.

Phase 1 Flink job:

```text
Kafka source: bluesky.raw.v1
Iceberg sink: bluesky.bronze_raw
checkpoint storage: s3://flink-checkpoints
savepoint storage: s3://flink-savepoints
```

## Step 7. Producer 배포

Producer는 Bluesky Jetstream WebSocket에서 raw event를 받아 Kafka topic에 write한다.

초기 payload는 원본 보존을 우선한다.

```json
{
  "source": "bluesky_jetstream",
  "producer_ts": "2026-05-26T00:00:00Z",
  "raw": {}
}
```

## Step 8. Bronze 조회 확인

Trino에서 Iceberg Bronze table을 조회한다.

```sql
SELECT count(*)
FROM bluesky.bronze_raw;
```

시간대별 수집량:

```sql
SELECT
  dt,
  hour,
  count(*) AS events
FROM bluesky.bronze_raw
GROUP BY dt, hour
ORDER BY dt, hour;
```

## Phase 1 완료 기준

- 두 번째 노드가 Kubernetes cluster에 `Ready` 상태로 join되어 있다.
- `streaming`, `processing`, `lakehouse`, `observability` namespace가 존재한다.
- Kafka topic `bluesky.raw.v1`이 생성되어 있다.
- Producer가 Bluesky 이벤트를 Kafka에 write한다.
- Flink job이 Kafka topic을 consume한다.
- Iceberg Bronze table에 raw event가 저장된다.
- Trino에서 `SELECT count(*) FROM bluesky.bronze_raw`가 성공한다.
- 최소 6시간, 가능하면 24시간 이상 raw 데이터를 수집한다.
