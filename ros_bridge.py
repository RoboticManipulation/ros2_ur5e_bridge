import rclpy
from rclpy.node import Node
from rclpy.action import ActionClient

from control_msgs.action import FollowJointTrajectory
from trajectory_msgs.msg import JointTrajectoryPoint
from rclpy.executors import  SingleThreadedExecutor
import threading
import json
import redis

from tf2_ros import TransformListener, Buffer
from geometry_msgs.msg import TransformStamped
from rclpy.node import Node
import time


from geometry_msgs.msg import TwistStamped
import tf2_ros
from tf2_ros import TransformException
import numpy as np
from geometry_msgs.msg import Vector3

REDIS_HOST = "172.17.0.1"  
REDIS_PORT = 6379

class TFToRedisPublisher(Node):
    def __init__(self, redis_client):
        super().__init__('tf_to_redis_publisher')
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)
        self.redis_client = redis_client

        self.timer = self.create_timer(0.5, self.publish_tf_to_redis)  # every 0.5s

    def publish_tf_to_redis(self):
        try:
            now = rclpy.time.Time()
            trans: TransformStamped = self.tf_buffer.lookup_transform(
                'base_link',  # parent frame
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
            self.get_logger().info(f"Published TF to Redis : {tf_data}")

        except Exception as e:
            self.get_logger().warn(f"Failed to get transform: {e}")


class URController(Node):
    def __init__(self):
        super().__init__('follow_joint_trajectory_sync_client')
        # Create the action client for FollowJointTrajectory
        self._client = ActionClient(
            self,
            FollowJointTrajectory,
            '/joint_trajectory_controller/follow_joint_trajectory'
        )

    def send_goal_sync(self,joint_positions):
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
        point.time_from_start.sec = 3  # 5 seconds
        point.time_from_start.nanosec = 0

        goal_msg.trajectory.points.append(point)

        # Send goal asynchronously, but block (spin) until it completes
        self.get_logger().info('Sending goal...')
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
                        linear_speed = np.linalg.norm(linear_velocity)
                        angular_speed = np.linalg.norm(angular_velocity)
                        end_effector_speed = {
                            'linear_speed': linear_speed,
                            'angular_speed': angular_speed,
                        }

                        self.redis_client.set("end_effector_speed", json.dumps(end_effector_speed))
                        self.get_logger().info(
                            f'Linear velocity: {linear_speed:.3f} m/s, Angular velocity: {angular_speed:.3f} rad/s'
                        )
            
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


class ROSBridge:
  
    def __init__(self):
        rclpy.init()
  
        self.joint_trajectory_client = URController()
        self.robot_joint_position = []
        self.executor1 = SingleThreadedExecutor()
       

        self.executor1.add_node(self.joint_trajectory_client)
        self.executor_thread1 = threading.Thread(target=self.spin_executor1, daemon=True)     
        self.executor_thread1.start()
        
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.tf_publisher = TFToRedisPublisher(self.redis_client)
        
        
        self.executor2 = SingleThreadedExecutor()
        self.executor2.add_node(self.tf_publisher)
        self.executor_thread2 = threading.Thread(target=self.spin_executor2, daemon=True)     
        self.executor_thread2.start()
        
        self.gripper_velocity_tracker=GripperVelocityTracker(self.redis_client)
        
        self.executor3 = SingleThreadedExecutor()
        self.executor3.add_node(self.gripper_velocity_tracker)
        self.executor_thread3 = threading.Thread(target=self.spin_executor3, daemon=True)     
        self.executor_thread3.start()


        
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
            
            
    def publish_joint_angles(self, joint_positions):
        self.joint_trajectory_client.send_goal_sync(joint_positions)

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
    bridge = ROSBridge()
    print('Running UR5e Bridge')
    
    while True:
        # Block's until a message arrives on "task_queue"
        _, message = r.blpop("joints_queue")
        print(f"Received task: {message}")

        try:
            joint_positions = json.loads(message)
            joint_positions[0] = joint_positions[0] + (3.14/2) # To correct the Robot Orientation on the table 
            joint_positions[5] = joint_positions[5] + (3.14/4) # To correct the Gripper Orientation 
    
    
            joint_positions[1] = joint_positions[1] - 0.2  # TODO : Remove this (It exists To avoid the gripper crash with the table) 
            bridge.publish_joint_angles(joint_positions)
            
            response = "successfully published joints"
                
        except (json.JSONDecodeError, TypeError):

            response = "Something wrong publishing joints"

        # Pushing the response to "joints_response_queue"
        r.lpush("joints_response_queue", response)
        