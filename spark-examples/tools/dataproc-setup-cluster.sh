gcloud dataproc clusters create spark-demo \
  --image-version 1.1 \
  --master-machine-type n1-standard-4 \
  --master-boot-disk-size 10 \
  --num-workers 2 \
  --worker-machine-type n1-standard-4 \
  --worker-boot-disk-size 10 \
  --scopes 'https://www.googleapis.com/auth/cloud-platform'
