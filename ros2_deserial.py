from rosidl_runtime_py.utilities import get_message
import rosbag2_py
from rclpy.serialization import deserialize_message

# ros2 galactic and colcon build

reader = rosbag2_py.SequentialReader()
options = rosbag2_py.StorageOptions('/home/krri/bag_check/rcl_test/src/ros_deserial/ros_deserial/rosbag2_2021_04_15-16_34_39', "sqlite3")
converter = rosbag2_py.ConverterOptions("cdr", "cdr")

reader.open(options, converter)
i = 0 
type_map = reader.get_all_topics_and_types()
print(type_map[0].type) # check get_all_topics_and_types()
while reader.has_next():
    print(f'{i}')
    i += 1
    topic, data, timestamp = reader.read_next()
    # msg_type = get_message(topic)
    # msg_type = get_message(type_map[topic])
    # msg = deserialize_message(data, msg_type)
    # print(msg)