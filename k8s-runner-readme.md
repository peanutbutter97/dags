# Update ConfigMap
kubectl create configmap hello-src --from-file=hello_src/ -n airflow-k8s-task -o yaml --dry-run=client | kubectl apply -f -