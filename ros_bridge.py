import rclpy
from rclpy.node import Node
from rclpy.action import ActionClient

from control_msgs.action import FollowJointTrajectory
from trajectory_msgs.msg import JointTrajectoryPoint
from rclpy.executors import  SingleThreadedExecutor
import threading
import json
import redis

from tf2_ros import Buffer, TransformListener, LookupException, ConnectivityException, ExtrapolationException
from geometry_msgs.msg import TransformStamped
from rclpy.node import Node
import time


from geometry_msgs.msg import TwistStamped
import tf2_ros
from tf2_ros import TransformException
import numpy as np
from geometry_msgs.msg import Vector3


from sensor_msgs.msg import PointCloud2,Image
import sensor_msgs_py.point_cloud2 as pc2
import open3d as o3d

from scipy.spatial.transform import Rotation as R
from geometry_msgs.msg import TransformStamped
import zlib
import pickle
from cv_bridge import CvBridge
import cv2


REDIS_HOST = "172.17.0.1"  
REDIS_PORT = 6379

class EndEffectorTFToRedisPublisher(Node):
    def __init__(self, redis_client):
        super().__init__('tf_to_redis_publisher')
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)
        self.redis_client = redis_client

        self.timer = self.create_timer(0.025, self.publish_tf_to_redis)  # every 0.5s
        self.timer = self.create_timer(0.025, self.publish_tool0_tf_to_redis)  # every 0.5s
        self.timer = self.create_timer(0.025, self.publish_tf_wrt_sandboxcenter_to_redis)  # every 0.5s
        
        # Buffers to store samples
        # self.position_samples = []
        # self.orientation_samples = []
        # self.max_samples = 100  # Number of samples to average

    def publish_tf_to_redis(self):
        try:
            now = rclpy.time.Time()
            trans: TransformStamped = self.tf_buffer.lookup_transform(
                'mujuco_world',  # parent frame
                'custom_gripper_grasp_point',  # child frame
                now,
                timeout=rclpy.duration.Duration(seconds=1.0)
            )
            timestamp = trans.header.stamp.sec + trans.header.stamp.nanosec * 1e-9
            position = {
                'x': trans.transform.translation.x,
                'y': trans.transform.translation.y,
                'z': trans.transform.translation.z
            }

            orientation = {
                'x': trans.transform.rotation.x,
                'y': trans.transform.rotation.y,
                'z': trans.transform.rotation.z,
                'w': trans.transform.rotation.w
            }

            tf_data = {
                'position': position,
                'orientation': orientation,
                'timestamp': timestamp,
            }

            self.redis_client.set("custom_gripper_grasp_point_tf", json.dumps(tf_data))
            # self.get_logger().info(f"Published TF to Redis : {tf_data}")

        except Exception as e:
            self.get_logger().warn(f"Failed to get transform: {e}")
    
    # def publish_tf_to_redis(self):
    #     try:
    #         now = rclpy.time.Time()
    #         trans: TransformStamped = self.tf_buffer.lookup_transform(
    #             'mujuco_world',  # parent frame
    #             'custom_gripper_grasp_point',  # child frame
    #             now,
    #             timeout=rclpy.duration.Duration(seconds=1.0)
    #         )

    #         # Collect position and orientation samples
    #         self.position_samples.append([
    #             trans.transform.translation.x,
    #             trans.transform.translation.y,
    #             trans.transform.translation.z
    #         ])
    #         self.orientation_samples.append([
    #             trans.transform.rotation.x,
    #             trans.transform.rotation.y,
    #             trans.transform.rotation.z,
    #             trans.transform.rotation.w
    #         ])

    #         # Check if we have enough samples
    #         if len(self.position_samples) >= self.max_samples:
    #             # Calculate average position
    #             avg_position = np.mean(self.position_samples, axis=0)
    #             avg_orientation = np.mean(self.orientation_samples, axis=0)
    #             avg_orientation /= np.linalg.norm(avg_orientation)  # Normalize quaternion

    #             # Create averaged transform data
    #             timestamp = trans.header.stamp.sec + trans.header.stamp.nanosec * 1e-9
    #             tf_data = {
    #                 'position': {
    #                     'x': avg_position[0],
    #                     'y': avg_position[1],
    #                     'z': avg_position[2]
    #                 },
    #                 'orientation': {
    #                     'x': avg_orientation[0],
    #                     'y': avg_orientation[1],
    #                     'z': avg_orientation[2],
    #                     'w': avg_orientation[3]
    #                 },
    #                 'timestamp': timestamp,
    #             }

    #             # Publish to Redis
    #             self.redis_client.set("custom_gripper_grasp_point_tf", json.dumps(tf_data))
    #             self.get_logger().info(f"Published averaged TF to Redis: {tf_data}")

    #             # Clear samples
    #             self.position_samples.clear()
    #             self.orientation_samples.clear()

    #     except Exception as e:
    #         self.get_logger().warn(f"Failed to get transform: {e}")

    def publish_tool0_tf_to_redis(self):
        try:
            now = rclpy.time.Time()
            trans: TransformStamped = self.tf_buffer.lookup_transform(
                'mujuco_world',  # parent frame
                'tool0',  # child frame
                now,
                timeout=rclpy.duration.Duration(seconds=1.0)
            )
            timestamp = trans.header.stamp.sec + trans.header.stamp.nanosec * 1e-9
            position = {
                'x': trans.transform.translation.x,
                'y': trans.transform.translation.y,
                'z': trans.transform.translation.z
            }

            orientation = {
                'x': trans.transform.rotation.x,
                'y': trans.transform.rotation.y,
                'z': trans.transform.rotation.z,
                'w': trans.transform.rotation.w
            }

            tf_data = {
                'position': position,
                'orientation': orientation,
                'timestamp': timestamp,
            }

            self.redis_client.set("tool0_tf", json.dumps(tf_data))
            # self.get_logger().info(f"Published Tool 0 TF to Redis : {tf_data}")

        except Exception as e:
            self.get_logger().warn(f"Failed to get transform: {e}")
    
    def publish_tf_wrt_sandboxcenter_to_redis(self):
        try:
            now = rclpy.time.Time()
            trans: TransformStamped = self.tf_buffer.lookup_transform(
                'simulation_sand_box_center',  # parent frame
                'custom_gripper_grasp_point',  # child frame
                now,
                timeout=rclpy.duration.Duration(seconds=1.0)
            )
            timestamp = trans.header.stamp.sec + trans.header.stamp.nanosec * 1e-9
            position = {
                'x': trans.transform.translation.x,
                'y': trans.transform.translation.y,
                'z': trans.transform.translation.z
            }

            orientation = {
                'x': trans.transform.rotation.x,
                'y': trans.transform.rotation.y,
                'z': trans.transform.rotation.z,
                'w': trans.transform.rotation.w
            }

            tf_data = {
                'position': position,
                'orientation': orientation,
                'timestamp': timestamp,
            }

            self.redis_client.set("custom_gripper_grasp_point_tf_wrt_sandbox_center", json.dumps(tf_data))
            # self.get_logger().info(f"Published TF to Redis : {tf_data}")

        except Exception as e:
            self.get_logger().warn(f"Failed to get transform: {e}")


class URController(Node):
    def __init__(self, redis_client):
        super().__init__('follow_joint_trajectory_sync_client')
        # Create the action client for FollowJointTrajectory
        self._client = ActionClient(
            self,
            FollowJointTrajectory,
            '/joint_trajectory_controller/follow_joint_trajectory'
        )
        self.redis_client = redis_client

    def send_goal_sync(self,joint_positions,time_for_execution=3):
        """Send the trajectory goal synchronously and print the result."""
        # Wait until the action server is available
        self._client.wait_for_server()

        # Create a goal message
        goal_msg = FollowJointTrajectory.Goal()
        goal_msg.trajectory.joint_names = [
            'shoulder_pan_joint',
            'shoulder_lift_joint',
            'elbow_joint',
            'wrist_1_joint',
            'wrist_2_joint',
            'wrist_3_joint'
           
        ]

        # Create a single trajectory point
        point = JointTrajectoryPoint()
        point.positions = joint_positions
        point.time_from_start.sec = time_for_execution  # 5 seconds
        point.time_from_start.nanosec = 0

        goal_msg.trajectory.points.append(point)

        # Send goal asynchronously, but block (spin) until it completes
        self.get_logger().info('Sending goal...')
        print(f"Goal message : {goal_msg}")
        send_goal_future = self._client.send_goal_async(goal_msg)
        rclpy.spin_until_future_complete(self, send_goal_future)

        # Check if the goal was accepted
        goal_handle = send_goal_future.result()
        if not goal_handle.accepted:
            self.get_logger().error('Goal was rejected by the action server.')
            print('Goal was rejected by the action server.') 
            return
        self.get_logger().info('Goal accepted by the action server.')
        print(f'Goal accepted by the action server.') 

        # Wait for the result
        get_result_future = goal_handle.get_result_async()
        rclpy.spin_until_future_complete(self, get_result_future)

        result = get_result_future.result().result
        self.get_logger().info(f'Goal finished. Result: {result}')
        print(f'Goal finished. Result: {result}') 
        response = f'Sending Back Response: Goal finished. Result: {result}'
        print(response)
        self.redis_client.lpush("joints_response_queue", response)


class GripperVelocityTracker(Node):
    def __init__(self, redis_client):
        super().__init__('gripper_velocity_tracker')
        
        self.redis_client = redis_client
        
        # Parameters
        self.frame_id = 'custom_gripper_grasp_point'
        self.reference_frame = 'base_link'  # Base frame of the robot
        self.update_rate = 100.0  # Hz
        
        # Publishers
        self.velocity_pub = self.create_publisher(
            TwistStamped, 
            '/gripper_velocity', 
            10
        )
        
        # Set up TF2 buffer and listener
        self.tf_buffer = tf2_ros.Buffer()
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer, self)
        
        # Store previous transform and timestamp
        self.prev_transform = None
        self.prev_timestamp = None
        
        # Create timer
        self.timer = self.create_timer(1.0/self.update_rate, self.compute_velocity)
        
        self.get_logger().info('Gripper velocity tracker initialized')
    
    def compute_velocity(self):
        """Compute and publish the linear and angular velocity of the end effector."""
        try:
            # Get the current transform
            current_transform = self.tf_buffer.lookup_transform(
                self.reference_frame,
                self.frame_id,
                rclpy.time.Time()
            )
            
            current_timestamp = current_transform.header.stamp.sec + current_transform.header.stamp.nanosec * 1e-9
            
            if self.prev_transform is not None and self.prev_timestamp is not None:
                # Time delta
                dt = current_timestamp - self.prev_timestamp
                
                if dt > 0:
                    # Extract positions
                    current_pos = np.array([
                        current_transform.transform.translation.x,
                        current_transform.transform.translation.y,
                        current_transform.transform.translation.z
                    ])
                    
                    prev_pos = np.array([
                        self.prev_transform.transform.translation.x,
                        self.prev_transform.transform.translation.y,
                        self.prev_transform.transform.translation.z
                    ])
                    
                    # Calculate linear velocity (m/s)
                    linear_velocity = (current_pos - prev_pos) / dt
                    
                    # Extract quaternions
                    current_quat = np.array([
                        current_transform.transform.rotation.x,
                        current_transform.transform.rotation.y,
                        current_transform.transform.rotation.z,
                        current_transform.transform.rotation.w
                    ])
                    
                    prev_quat = np.array([
                        self.prev_transform.transform.rotation.x,
                        self.prev_transform.transform.rotation.y,
                        self.prev_transform.transform.rotation.z,
                        self.prev_transform.transform.rotation.w
                    ])
                    
                    # Calculate angular velocity from quaternions
                    angular_velocity = self.quaternion_to_angular_velocity(prev_quat, current_quat, dt)
                    
                    # Create and publish the twist message
                    twist_msg = TwistStamped()
                    twist_msg.header.stamp = self.get_clock().now().to_msg()
                    twist_msg.header.frame_id = self.reference_frame
                    
                    # Linear velocity
                    twist_msg.twist.linear.x = linear_velocity[0]
                    twist_msg.twist.linear.y = linear_velocity[1]
                    twist_msg.twist.linear.z = linear_velocity[2]
                    
                    # Angular velocity
                    twist_msg.twist.angular.x = angular_velocity[0]
                    twist_msg.twist.angular.y = angular_velocity[1]
                    twist_msg.twist.angular.z = angular_velocity[2]
                    
                    # Publish the message
                    self.velocity_pub.publish(twist_msg)
                    
                    # Log velocities at a reduced rate (every second)
                    if int(current_timestamp) > int(self.prev_timestamp):
                        # linear_speed = np.linalg.norm(linear_velocity)
                        # angular_speed = np.linalg.norm(angular_velocity)
                        # end_effector_speed = {
                        #   'linear_speed': round(linear_speed, 3),
                        #   'angular_speed': round(angular_speed, 3),
                        # }
                        
                        end_effector_speed = {
                            'linear_velocity': {
                                'x': round(linear_velocity[0], 3),
                                'y': round(linear_velocity[1], 3),
                                'z': round(linear_velocity[2], 3)
                            },
                            'angular_velocity': {
                                'x': round(angular_velocity[0], 3),
                                'y': round(angular_velocity[1], 3),
                                'z': round(angular_velocity[2], 3)
                            }
                        }

                        self.redis_client.set("end_effector_speed", json.dumps(end_effector_speed))
                        
                        # For logging purposes, calculate the magnitudes
                        linear_speed = np.linalg.norm(linear_velocity)
                        angular_speed = np.linalg.norm(angular_velocity)
                        
                        # self.get_logger().info(
                        #     f'Linear velocity: {linear_speed:.3f} m/s, Angular velocity: {angular_speed:.3f} rad/s'
                        # )
            
            # Update previous transform and timestamp
            self.prev_transform = current_transform
            self.prev_timestamp = current_timestamp
            
        except TransformException as ex:
            self.get_logger().warning(f'Could not get transform: {ex}')
    
    def quaternion_to_angular_velocity(self, q1, q2, dt):
        """
        Calculate angular velocity from two quaternions.
        
        Args:
            q1 (numpy.ndarray): Previous quaternion [x, y, z, w]
            q2 (numpy.ndarray): Current quaternion [x, y, z, w]
            dt (float): Time difference in seconds
            
        Returns:
            numpy.ndarray: Angular velocity as [wx, wy, wz]
        """
        # Ensure unit quaternions
        q1 = q1 / np.linalg.norm(q1)
        q2 = q2 / np.linalg.norm(q2)
        
        # Calculate quaternion derivative (q̇ = (q2 - q1) / dt)
        # Use quaternion product for more accurate results
        # The quaternion derivative is related to angular velocity by: q̇ = 0.5 * ω ⊗ q
        # where ω is the pure quaternion form of the angular velocity [0, wx, wy, wz]
        
        # Compute the relative quaternion
        rel_quat = self.quaternion_multiply(self.quaternion_inverse(q1), q2)
        
        # Extract the rotation angle
        angle = 2 * np.arccos(rel_quat[3])  # w component
        
        # Handle small angles
        if abs(angle) < 1e-10:
            return np.array([0.0, 0.0, 0.0])
        
        # Extract the rotation axis
        axis = rel_quat[0:3] / np.sin(angle/2)
        
        # Scale by angle/dt to get angular velocity
        angular_velocity = axis * (angle / dt)
        
        return angular_velocity
    
    def quaternion_multiply(self, q1, q2):
        """
        Multiply two quaternions.
        
        Args:
            q1 (numpy.ndarray): First quaternion [x, y, z, w]
            q2 (numpy.ndarray): Second quaternion [x, y, z, w]
            
        Returns:
            numpy.ndarray: Result quaternion [x, y, z, w]
        """
        x1, y1, z1, w1 = q1
        x2, y2, z2, w2 = q2
        
        return np.array([
            w1*x2 + x1*w2 + y1*z2 - z1*y2,
            w1*y2 - x1*z2 + y1*w2 + z1*x2,
            w1*z2 + x1*y2 - y1*x2 + z1*w2,
            w1*w2 - x1*x2 - y1*y2 - z1*z2
        ])
    
    def quaternion_inverse(self, q):
        """
        Compute the inverse of a quaternion.
        
        Args:
            q (numpy.ndarray): Quaternion [x, y, z, w]
            
        Returns:
            numpy.ndarray: Inverse quaternion [x, y, z, w]
        """
        return np.array([-q[0], -q[1], -q[2], q[3]]) / np.sum(q**2)



# class SandBoxTransformPublisherNode(Node):
#     def __init__(self):
#         super().__init__('sandbox_transform_publisher_node')
        
#         # Create a static transform broadcaster
#         self.tf_broadcaster = tf2_ros.StaticTransformBroadcaster(self)
        
#         # Define the transforms to publish
#         transforms = []
        
#         # Parent frame
#         parent_frame = 'tagStandard52h13:3'
        
#         # Define offsets as (x, y, z) tuples
#         offsets = [
#             (0.0, 0.0, 0.0),
#             (0.3, 0.0, 0.0),
#             (0.0, 0.6, 0.0),
#             (0.3, 0.6, 0.0),
            
#             (0.0, 0.0, -0.09),
#             (0.3, 0.0, -0.09),
#             (0.0, 0.6, -0.09),
#             (0.3, 0.6, -0.09)
#         ]
        
#         # Create the transforms
#         for i, offset in enumerate(offsets):
#             # Create a transform message
#             transform = TransformStamped()
            
#             # Set header information
#             transform.header.stamp = self.get_clock().now().to_msg()
#             transform.header.frame_id = parent_frame
            
#             # Set child frame with a unique name based on the offset
#             transform.child_frame_id = f'sand_box_frame_{i+1}'
            
#             # Set translation (x, y, z)
#             transform.transform.translation.x = offset[0]
#             transform.transform.translation.y = offset[1]
#             transform.transform.translation.z = offset[2]
            
#             # Set rotation as identity quaternion (no rotation)
#             transform.transform.rotation.x = 0.0
#             transform.transform.rotation.y = 0.0
#             transform.transform.rotation.z = 0.0
#             transform.transform.rotation.w = 1.0
            
#             transforms.append(transform)
            
#             self.get_logger().info(f'Created transform: {transform.header.frame_id} -> {transform.child_frame_id}')
#             self.get_logger().info(f'Translation: ({offset[0]}, {offset[1]}, {offset[2]})')
        
#         # Broadcast all transforms
#         self.tf_broadcaster.sendTransform(transforms)
#         self.get_logger().info(f'Published {len(transforms)} static transforms')
        
        
class SandBoxPointCloudSegmentation(Node):
    def __init__(self):
        super().__init__('pointcloud_segmentation_node')
        self.subscription = self.create_subscription(
            PointCloud2,
            '/zed2i/zed_node/point_cloud/cloud_registered',
            self.listener_callback,
            10)
        # self.subscription  
        
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)
        
        self.pub_cropped = self.create_publisher(
            PointCloud2,
            '/segmented_sandbox_pointcloud',
            10
        )
        self.latest_segmented_point_cloud = []



    def listener_callback(self, msg):
        # self.get_logger().info('Received point cloud')

        points = []
        for p in pc2.read_points(msg, field_names=("x", "y", "z"), skip_nans=True):
            x, y, z = map(float, p)
            if not (np.isnan(x) or np.isnan(y) or np.isnan(z)) and \
            not (np.isinf(x) or np.isinf(y) or np.isinf(z)):
                points.append([x, y, z])

        cloud_points = np.array(points, dtype=np.float64)



        # self.get_logger().info(f"PointCloud shape: {cloud_points.shape}, dtype: {cloud_points.dtype}")

        if cloud_points.size == 0:
            self.get_logger().warn("PointCloud is empty. Skipping visualization.")
            return

        # Look up the transform
        try:
            transform: TransformStamped = self.tf_buffer.lookup_transform(
                target_frame='simulation_sand_box_center',#'sandbox_center',
                source_frame='zed2i_left_camera_frame',
                time=rclpy.time.Time(),
                timeout=rclpy.duration.Duration(seconds=1.0)
            )

            # Extract translation and rotation
            t = transform.transform.translation
            q = transform.transform.rotation

            translation = np.array([t.x, t.y, t.z])
            rotation = R.from_quat([q.x, q.y, q.z, q.w]).as_matrix()

            # Apply transform
            transformed_points = (rotation @ cloud_points.T).T + translation
            
            
        except (LookupException, ConnectivityException, ExtrapolationException) as e:
            self.get_logger().warn(f"Transform failed: {e}")
            return

        try:
            pcd = o3d.geometry.PointCloud()
            pcd.points = o3d.utility.Vector3dVector(transformed_points)
            # pcd.points = o3d.utility.Vector3dVector(cloud_points)
            
            bbox = o3d.geometry.AxisAlignedBoundingBox(
                min_bound=(-0.15, -0.3, 0.0),  # bottom corner
                max_bound=(0.15, 0.3, 0.1)     # top corner
            )
            bbox.color = (0, 0, 1)  # Blue box
            pcd = pcd.crop(bbox)
            


            # o3d.io.write_point_cloud("output_transformed.ply", pcd)
            # o3d.visualization.draw_geometries([pcd, bbox])
            # Convert Open3D point cloud to NumPy array
            cropped_np = np.asarray(pcd.points)
            self.latest_segmented_point_cloud = cropped_np

            # Create header
            from std_msgs.msg import Header
            header = Header()
            header.stamp = self.get_clock().now().to_msg()
            header.frame_id = 'simulation_sand_box_center'  
            # Convert to PointCloud2
            msg_out = pc2.create_cloud_xyz32(header, cropped_np.tolist())

            # Publish
            self.pub_cropped.publish(msg_out)
            

        except Exception as e:
            self.get_logger().error(f"Open3D error: {e}")
            return
        
        # Uncomment to exit 
        # self.destroy_node()
        
    def get_latest_pointcloud(self):
        return zlib.compress(pickle.dumps(self.latest_segmented_point_cloud))
    

from tf2_ros import TransformBroadcaster, Buffer, TransformListener
from geometry_msgs.msg import TransformStamped
from rclpy.duration import Duration
from rclpy.time import Time


# class FrameDifferencePublisher(Node):
#     def __init__(self):
#         super().__init__('frame_difference_publisher')
#         self.tf_buffer = Buffer()
#         self.tf_listener = TransformListener(self.tf_buffer, self)

#         self.broadcaster = TransformBroadcaster(self)

#         self.timer = self.create_timer(0.1, self.publish_difference)

#     def publish_difference(self):
#         try:
#             # Get transform from sand_box_center to simulation_sand_box_center
#             transform = self.tf_buffer.lookup_transform(
#                 target_frame='sandbox_center',
#                 source_frame='simulation_sand_box_center',
#                 time=Time(),
#                 timeout=Duration(seconds=1.0)
#             )

#             # Modify the frame_ids to indicate it's the difference
#             transform.header.stamp = self.get_clock().now().to_msg()
#             transform.header.frame_id = 'sand_box_center'
#             transform.child_frame_id = 'difference_simulation_to_real'

#             # Broadcast the difference transform
#             self.broadcaster.sendTransform(transform)

#         except Exception as e:
#             self.get_logger().warn(f'Could not find transform: {e}')

# class FrameDifferencePublisher(Node):
#     def __init__(self):
#         super().__init__('frame_difference_publisher')
#         self.tf_buffer = Buffer()
#         self.tf_listener = TransformListener(self.tf_buffer, self)

#         self.broadcaster = TransformBroadcaster(self)

#         self.timer = self.create_timer(0.1, self.publish_lookup)

#     def publish_lookup(self):
#         try:
#             # Get transform from sand_box_center to simulation_sand_box_center
#             transform = self.tf_buffer.lookup_transform(
#                 target_frame='custom_gripper_grasp_point',
#                 source_frame='mujuco_world',
#                 time=Time(),
#                 timeout=Duration(seconds=1.0)
#             )

           

#         except Exception as e:
#             self.get_logger().warn(f'Could not find transform: {e}')

from geometry_msgs.msg import PoseStamped

class FrameDifferencePublisher(Node):
    def __init__(self):
        super().__init__('frame_difference_publisher')
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)

      
        # Create a publisher for PoseStamped
        self.pose_publisher = self.create_publisher(PoseStamped, '/transform_between_mujuco_world_to_custom_gripper_grasp_point', 10)

        self.timer = self.create_timer(0.1, self.publish_lookup)

    def publish_lookup(self):
        try:
            # Get transform from sand_box_center to simulation_sand_box_center
            transform = self.tf_buffer.lookup_transform(
                target_frame='custom_gripper_grasp_point',
                source_frame='mujuco_world',
                time=Time(),
                timeout=Duration(seconds=1.0)
            )

            # Convert the transform to a PoseStamped message
            pose_msg = PoseStamped()
            pose_msg.header.stamp = self.get_clock().now().to_msg()
            pose_msg.header.frame_id = 'mujuco_world'

            pose_msg.pose.position.x = transform.transform.translation.x
            pose_msg.pose.position.y = transform.transform.translation.y
            pose_msg.pose.position.z = transform.transform.translation.z

            pose_msg.pose.orientation.x = transform.transform.rotation.x
            pose_msg.pose.orientation.y = transform.transform.rotation.y
            pose_msg.pose.orientation.z = transform.transform.rotation.z
            pose_msg.pose.orientation.w = transform.transform.rotation.w

            # Publish the PoseStamped message
            self.pose_publisher.publish(pose_msg)
            # self.get_logger().info(f'Published PoseStamped: {pose_msg}')

        except Exception as e:
            self.get_logger().warn(f'Could not find transform: {e}')


import rclpy
from rclpy.node import Node
from tf2_ros import TransformListener, Buffer
import numpy as np
from geometry_msgs.msg import TransformStamped



class TransformAverager(Node):
    def __init__(self):
        super().__init__('transform_averager')
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)
        self.timer = self.create_timer(0.01, self.timer_callback)

        self.samples = []
        self.target_frame= 'tagStandard52h13:3'
        self.source_frame = 'zed2i_left_camera_optical_frame'
        self.max_samples = 1000

    def timer_callback(self):
        try:
            now = rclpy.time.Time()
            trans = self.tf_buffer.lookup_transform(
                self.source_frame,
                self.target_frame,
                now)
            
            self.samples.append(trans)

            if len(self.samples) >= self.max_samples:
                self.timer.cancel()
                self.average_transform()

        except Exception as e:
            self.get_logger().info(f'Transform not available yet: {e}')

    def average_transform(self):
        translations = np.array([[t.transform.translation.x,
                                  t.transform.translation.y,
                                  t.transform.translation.z] for t in self.samples])
        
        quaternions = np.array([[t.transform.rotation.x,
                                 t.transform.rotation.y,
                                 t.transform.rotation.z,
                                 t.transform.rotation.w] for t in self.samples])
        
        avg_translation = np.mean(translations, axis=0)
        
        # Average quaternions via normalized sum (approximation)
        avg_quat = np.mean(quaternions, axis=0)
        avg_quat = avg_quat / np.linalg.norm(avg_quat)

        self.get_logger().info(f"Averaged Translation: {avg_translation}")
        self.get_logger().info(f"Averaged Quaternion: {avg_quat}")

class ZEDCameraStreaming(Node):
    def __init__(self,redis_client):
        super().__init__('image_redis_streamer')
        self.bridge = CvBridge()
        self.redis_client = redis_client
        self.subscription = self.create_subscription(
            Image,
            '/zed2i/zed_node/left/image_rect_color',
            self.image_callback,
            10)

    def image_callback(self, msg):
        try:
            cv_image = self.bridge.imgmsg_to_cv2(msg, desired_encoding='bgr8')
            _, encoded_img = cv2.imencode('.jpg', cv_image)
            self.redis_client.set('zed2i_image_frame', encoded_img.tobytes())
        except Exception as e:
            self.get_logger().error(f"Error: {e}")


class ROSBridge:
  
    def __init__(self,redis_client):
        rclpy.init()
  
        self.redis_client = redis_client #redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.joint_trajectory_client = URController(self.redis_client)
        self.robot_joint_position = []
        self.executor1 = SingleThreadedExecutor()
        self.executor1.add_node(self.joint_trajectory_client)
        self.executor_thread1 = threading.Thread(target=self.spin_executor1, daemon=True)     
        self.executor_thread1.start()
        
        
        
        self.tf_publisher = EndEffectorTFToRedisPublisher(self.redis_client)
        self.executor2 = SingleThreadedExecutor()
        self.executor2.add_node(self.tf_publisher)
        self.executor_thread2 = threading.Thread(target=self.spin_executor2, daemon=True)     
        self.executor_thread2.start()
        
        # self.gripper_velocity_tracker=GripperVelocityTracker(self.redis_client)
        # self.executor3 = SingleThreadedExecutor()
        # self.executor3.add_node(self.gripper_velocity_tracker)
        # self.executor_thread3 = threading.Thread(target=self.spin_executor3, daemon=True)     
        # self.executor_thread3.start()
        
        # self.temporary_node = TransformAverager()
        # self.executor4 = SingleThreadedExecutor()
        # self.executor4.add_node(self.temporary_node)
        # self.executor_thread4 = threading.Thread(target=self.spin_executor4, daemon=True)     
        # self.executor_thread4.start()
        
        self.temporary_node = FrameDifferencePublisher()
        self.executor4 = SingleThreadedExecutor()
        self.executor4.add_node(self.temporary_node)
        self.executor_thread4 = threading.Thread(target=self.spin_executor4, daemon=True)     
        self.executor_thread4.start()
        
        self.point_cloud_segmentation = SandBoxPointCloudSegmentation()
        self.executor5 = SingleThreadedExecutor()
        self.executor5.add_node(self.point_cloud_segmentation)
        self.executor_thread5 = threading.Thread(target=self.spin_executor5, daemon=True)     
        self.executor_thread5.start()

        self.camera_streaming = ZEDCameraStreaming(self.redis_client)
        self.executor6 = SingleThreadedExecutor()
        self.executor6.add_node(self.camera_streaming)
        self.executor_thread6 = threading.Thread(target=self.spin_executor6, daemon=True)     
        self.executor_thread6.start()

        
    def spin_executor1(self):
        try:
            self.executor1.spin()
           
        except KeyboardInterrupt:
            pass
        finally:
            self.executor1.shutdown()
           
    def spin_executor2(self):
        try:
            self.executor2.spin()
           
        except KeyboardInterrupt:
            pass
        finally:
            self.executor2.shutdown()
            
    def spin_executor3(self):
        try:
            self.executor3.spin()
           
        except KeyboardInterrupt:
            pass
        finally:
            self.executor3.shutdown()
            
    def spin_executor4(self):
        try:
            self.executor4.spin()
           
        except KeyboardInterrupt:
            pass
        finally:
            self.executor4.shutdown()
            
    def spin_executor5(self):
        try:
            self.executor5.spin()
           
        except KeyboardInterrupt:
            pass
        finally:
            self.executor5.shutdown()
            
    def spin_executor6(self):
        try:
            self.executor6.spin()
           
        except KeyboardInterrupt:
            pass
        finally:
            self.executor6.shutdown()
            
            
    def publish_joint_angles(self, joint_positions,time_for_execution=3):
        self.joint_trajectory_client.send_goal_sync(joint_positions,time_for_execution)
        
    def get_latest_pointcloud(self):
        return self.point_cloud_segmentation.get_latest_pointcloud()

def main(args=None):

    # joint_positions = [
    #     0.87976908493042,   # shoulder_pan_joint
    #     -1.789188142816061,  # shoulder_lift_joint
    #     1.5098281065570276,  # elbow_joint
    #     -0.010845021610595751,  # wrist_1_joint
    #     1.440675139427185,   # wrist_2_joint
    #     1.690097689628601    # wrist_3_joint
    # ]
    
    joint_positions = [
        1.57,   # shoulder_pan_joint
        0.0,  # shoulder_lift_joint
        0.0,  # elbow_joint
        0.0,  # wrist_1_joint
        0.0,   # wrist_2_joint
        0.785   # wrist_3_joint
    ]

    # joint_positions = [
    #     0.0,   # shoulder_pan_joint
    #     0.0,  # shoulder_lift_joint
    #     0.0,  # elbow_joint
    #     0.0,  # wrist_1_joint
    #     0.0,   # wrist_2_joint
    #     0.785   # wrist_3_joint
    # ]

    
    bridge = ROSBridge()
    bridge.publish_joint_angles(joint_positions)
    pass

if __name__ == '__main__':
    # main()
  
    

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # Clear the queue before starting (to remove any message from previous sand_gym session)
    r.delete("joints_queue")
    bridge = ROSBridge(redis_client=r)
    print('Running UR5e Bridge')
    
    # while True:
       
    #     _, message = r.blpop("joints_queue")
    #     print(f"Received task: {message}")

    #     try:
    #         joint_positions = json.loads(message)
    #         joint_positions[0] = joint_positions[0] + (3.14/2) # To correct the Robot Orientation on the table 
    #         joint_positions[5] = joint_positions[5] + (3.14/4) # To correct the Gripper Orientation 
    
    
    #         # joint_positions[1] = joint_positions[1] - 0.2  # TODO : Remove this (It exists To avoid the gripper crash with the table) 
    #         bridge.publish_joint_angles(joint_positions)
            
    #         response = "successfully published joints"
                
    #     except (json.JSONDecodeError, TypeError):

    #         response = "Something wrong publishing joints"

    #     # Pushing the response to "joints_response_queue"
    #     r.lpush("joints_response_queue", response)
    
    while True:
        # Block for 1 sec waiting on either queue (non-blocking simulation)
        task_msg = r.blpop(["joints_queue", "pointcloud_request"], timeout=1)

        if task_msg:
            queue_name, message = task_msg

            if queue_name == "joints_queue":
                print(f"Received task: {message}")
                try:
                    
                   # Parse the payload
                    payload = json.loads(message)
                    joint_positions = payload["joint_angles"]
                    time_for_execution = payload["time_for_execution"]
                    # joint positions send from sand-gym mujuco
                    joint_positions[0] += (np.pi / 2)
                    joint_positions[5] += (np.pi / 4)
                
                    bridge.publish_joint_angles(joint_positions,int(time_for_execution))
                    # print(f"Published joint angles: {joint_positions}")
                    response = "successfully published joints"

                except (json.JSONDecodeError, TypeError):
                    response = "Something wrong publishing joints"

                # r.lpush("joints_response_queue", response)

            elif queue_name == "pointcloud_request":
                print("Received pointcloud request!")
                pc = bridge.get_latest_pointcloud()
                r.publish("pointcloud_data", pc)
        
