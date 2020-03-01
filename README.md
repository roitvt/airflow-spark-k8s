# airflow-spark-k8s
 This is a temporary home for Airflow -> Spark on Kubernetes integration.
 until PR: [[AIRFLOW-6542] add spark-on-k8s operator/hook/sensor](https://github.com/apache/airflow/pull/7163) will get merged

## Usage
1. Add the module: `airflow-spark-k8s` to Airflow requirements file.
2. Create airflow service-account in the Kubernetes cluster:
```bash
kubectl apply -f https://raw.githubusercontent.com/roitvt/airflow-spark-k8s/master/airflow-sa-rbac/airflow-cr.yaml
kubectl apply -f https://raw.githubusercontent.com/roitvt/airflow-spark-k8s/master/airflow-sa-rbac/airflow-crb.yaml
kubectl apply -f https://raw.githubusercontent.com/roitvt/airflow-spark-k8s/master/airflow-sa-rbac/airflow-sa.yaml
```  
3. Get airflow service account token:
```bash
kubectl get secret $(kubectl get sa airflow-sa -n spark-operator -ojsonpath="{.secrets[0].name}")  -n spark-operator -o jsonpath="{.data['token']}" | base64 --decode
```
5. Create new Airflow connection named: `kubernetes_default` with no connection type and the extra field set to: 

```json
{"extra__kubernetes__in_cluster": false, "extra__kubernetes__kube_config": "{\"apiVersion\":\"v1\",\"kind\":\"Config\",\"users\":[{\"name\":\"airflow-sa\",\"user\":{\"token\":\"<airflow-sa token>\"}}],\"clusters\":[{\"cluster\":{\"certificate-authority-data\":\"<cluster certificate authority>\",\"server\":\"<kubernetes api endpoint>\"},\"name\":\"<cluster-name>\"}],\"contexts\":[{\"context\":{\"cluster\":\"<cluster-name>\",\"user\":\"airflow-sa\"},\"name\":\"<context-name>\"}],\"current-context\":\"<context-name>\"}", "extra__kubernetes__namespace": "<default namespace>"}
