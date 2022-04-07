from rosidl_runtime_py.utilities import get_message
from rclpy.serialization import deserialize_message
import rosbag2_py
import open3d as o3d
from . import pointcloud2 as rnp
import os
import numpy as np
import cv2
import pandas as pd
import yaml

set_file_path = './src/ros2_bag_deserial/bag_setting/setting.yaml'

# ObsData sim_frame 기준
global cam_sim_frame
cam_sim_frame = '1'

class ReadBag():
    def __init__(self):
        bag_set = self.load_setting()
        self.save_dir = ''
        self.bag_uri = bag_set['bag_uri']
        self.topic_list = bag_set['lidar_topics'] + bag_set['cam_topics'] + bag_set['custom_topics'] + bag_set['csv_topics']
        self.lidar_topics = bag_set['lidar_topics']
        self.cam_topics = bag_set['cam_topics']
        self.csv_topics = bag_set['csv_topics']
        self.custom_topics = bag_set['custom_topics']
        self.option_list = bag_set['option_list']
        self.all_option = False
        self.sim_data = True #save sim_frame
        
    def load_setting(self):
        with open(set_file_path, 'r', encoding='utf-8') as f:
            bag_set = yaml.load(f, Loader=yaml.FullLoader)
            return bag_set

    # lidar bag to pcd and save
    def bag_to_pcd(self, xyz, color, t, dir_name):
        file_name = self.set_name(t, dir_name)
        file_name = file_name + ".pcd"
        pcd_xyz = o3d.geometry.PointCloud()
        pcd_xyz.points = o3d.utility.Vector3dVector(xyz)
        pcd_xyz.colors = o3d.utility.Vector3dVector(color)
        
        o3d.io.write_point_cloud(file_name, pcd_xyz)

    # lidar to numpy (get x, y, z, rgb point)
    # need data option select 
    def bag_lidar_to_numpy(self, lidar_msg, row_step):
        msg_np = rnp.pointcloud2_to_array(lidar_msg)
        msg_x = msg_np['x'].reshape(-1,1)
        msg_y = msg_np['y'].reshape(-1,1)
        msg_z = msg_np['z'].reshape(-1,1)
        msg_rgb = msg_np['rgb'].reshape(-1,1)
        msg_color = np.empty([row_step, 3], dtype=np.float32)
        msg_color[:] = msg_rgb[:]/-255
        msg_xyz = np.concatenate((msg_x,msg_y,msg_z), axis=1)
        
        return msg_xyz, msg_color
    
    # bag to img // 그냥 이미지
    # bag to img and save // 압축이미지
    def bag_to_img(self, cam_msg, t, dir_name):
        np_img = np.frombuffer(cam_msg.data, np.uint8)
        img = cv2.imdecode(np_img, cv2.IMREAD_COLOR)
        file_name = self.set_name(t, dir_name)
        cv2.imwrite(file_name+'.jpg', img)

    # msg data to csv and save    
    def msg_to_csv(self, msg, t, dir_name):
        attr_list = []
        if(self.all_option): # check all option or select option list
            for msg_attr in dir(msg):
                if(msg_attr[0] != '_'):
                    attr_list.append(msg_attr)
        else:
            for msg_attr in dir(msg):
                for option in self.option_list:
                    if(msg_attr == option):
                        attr_list.append(msg_attr)
        msg_data = []
        for attr in attr_list:
            msg_data.append(getattr(msg,attr))
            if(attr == 'sim_frame'): t = str(getattr(msg, attr))
        msg_data = pd.DataFrame(msg_data, index=attr_list)
        file_name = self.set_name(t, dir_name)
        msg_data.to_csv(file_name + '.csv', mode='w')
    
    '''
    obj_to_csv : custom msg topic function
    point_to_array : use obj get x, y, z points
    obj to csv and save
    '''
    def obj_to_csv(self, obs_msg, t, dir_name):
        global cam_sim_frame
        cam_sim_frame = str(obs_msg.sim_frame)
        # print(print(dir(obs_msg)))
        objs = obs_msg.obj
        attrs = [att for att in dir(objs[0]) if att.split('_')[0] == 'front' or att.split('_')[0] == 'rear']
        obj_arr = []
        index_arr = []
        for index, obj in enumerate(objs):
            for attr in attrs:
                xyz = self.point_to_array(getattr(obj, attr))
                obj_arr.append(xyz)
                index_arr.append(index)
        obj_df = pd.DataFrame(obj_arr, index=index_arr, columns=['x','y','z'])
        
        if(self.sim_data):
            file_name = self.set_name(cam_sim_frame, dir_name)
        else:
            file_name = self.set_name(t, dir_name)
        
        obj_df.to_csv(file_name +'.csv', mode='w')
    
    def point_to_array(self, point):
        xyz = np.array([point.x, point.y, point.z])
        return xyz

    # set file name before save
    def set_name(self, t, dir_name):
        if(self.sim_data):
            file_name = self.save_dir + dir_name + '/' + str(t)
        else:
            file_name = self.save_dir + dir_name + '/' + str(int(t/1000000))
        return file_name

    # def get_attr(msg_data):
    #     attr_list = []
    #     for attr in dir(msg_data):
    #         print(attr)
    #         if(attr[0] != '_'):
    #             attr_list.append(attr)

    #     return attr_list

    # read bag
    def run(self):
        options = rosbag2_py.StorageOptions('','')
        options.uri = self.bag_uri
        options.storage_id = 'sqlite3'

        converter = rosbag2_py.ConverterOptions('','')
        converter.input_serialization_format = 'cdr'
        converter.output_serialization_format = 'cdr'

        reader = rosbag2_py.SequentialReader()
        
        try:
            reader.open(options, converter)
        except Exception as e:
            print("error")

        topic_types = reader.get_all_topics_and_types()
        type_map = {topic_types[i].name : topic_types[i].type for i in range(len(topic_types))}

        storage_filter = rosbag2_py.StorageFilter(self.topic_list)
        reader.set_filter(storage_filter)
        
        global cam_sim_frame

        i = 0
        while reader.has_next():
            (topic, data, timestamp) = reader.read_next()
            msg_type = get_message(type_map[topic])
            msg = deserialize_message(data, msg_type)
            
            if(topic in self.lidar_topics):
                sim_frame = msg.header.stamp.sec
                msg_xyz, msg_color = self.bag_lidar_to_numpy(msg, msg.row_step)
                
                if(self.sim_data):
                    self.bag_to_pcd(msg_xyz, msg_color, sim_frame, topic) 
                else:
                    self.bag_to_pcd(msg_xyz, msg_color, timestamp, topic)
            
            if(topic in self.cam_topics):
                if(self.sim_data):
                    # global cam_sim_frame
                    self.bag_to_img(msg, cam_sim_frame, topic)
                else:
                    self.bag_to_img(msg, timestamp, topic)
            
            # ObsData
            if(topic in self.custom_topics):
                self.obj_to_csv(msg, timestamp, topic)
            
            # ego_pose
            if(topic in self.csv_topics):
                if(self.sim_data):
                    self.msg_to_csv(msg, cam_sim_frame, topic)
                else:
                    self.msg_to_csv(msg, timestamp, topic)
            
            i += 1
            print(f'processing..{i}\r', end='')

        print("complete")

# main func make dir before read bag
def main():
    readbag = ReadBag()
    
    bag_name = readbag.bag_uri.split('/')[-1]
    bag_name = bag_name.split('.')[0]
    
    data_dir = "./bag_data"
    if (not os.path.isdir(data_dir)):
        os.mkdir(data_dir)
        
    data_dir += "/" + bag_name
    readbag.save_dir = data_dir
    
    if (not os.path.isdir(data_dir)):
        os.mkdir(data_dir)
    
    for topic_name in readbag.topic_list:
        if(not os.path.isdir(data_dir + topic_name)):
            os.mkdir(data_dir + topic_name)
    
    readbag.run()
