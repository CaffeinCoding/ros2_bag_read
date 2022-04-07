# ros2_bag_read
- ros2 bag을 이용하여 녹화한 db3 파일에서 원하는 데이터를 추출하는 코드
- 현재는 Lidar, cam(compressed), csv 데이터만 추출가능
- Lidar는 pcd, Cam은 이미지, 나머지는 csv로 저장
- ros2 deserial의 참고 및 자체 프로젝트에 활용하기위해 제작된 코드이므로 다른 프로젝트에 적용이 어려울 수 있음