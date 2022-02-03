#!/usr/bin/env bash
mkdir -p /tmp/test_files
/tmp/esthri sync --source s3://sitl-data/sitl-runs/1457bc2c-db74-407a-9232-f39c407e5685/ --destination /tmp/test_files/run1
/tmp/esthri sync --source s3://sitl-data/sitl-runs/ded5bd92-a73b-42ad-8e04-5ae332edd7a8/ --destination /tmp/test_files/run2
/tmp/esthri sync --source s3://sitl-data/sitl-runs/96f12a7d-5931-4bb0-b591-055bae37b9f9/ --destination /tmp/test_files/run3
/tmp/esthri sync --source s3://sitl-data/sitl-runs/16af67e1-12a5-46e3-9323-8c1f2b57d8cb/ --destination /tmp/test_files/run4 
/tmp/esthri sync --source s3://sitl-data/sitl-runs/5056ad6e-d417-4f6f-b4f6-3d4973e0c401/ --destination /tmp/test_files/run5
/tmp/esthri sync --source s3://sitl-data/sitl-runs/6009659e-82aa-455c-bd33-8b14c99a4deb/ --destination /tmp/test_files/run6

aws.real s3 cp s3://sitl-data/sitl-runs/6a694303-48e3-41bd-be3b-18293ca9db48/pvt-driver-insights.results.json /tmp/test_files/

# sync this back up to s3
/tmp/esthri sync --destination s3://esthri-test/sam/testing/files/ --source /tmp/test_files/
/tmp/esthri sync --destination s3://esthri-test/sam/testing/files_compressed/ --source /tmp/test_files/ --transparent-compression