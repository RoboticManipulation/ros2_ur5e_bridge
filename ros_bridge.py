import rclpy
from rclpy.node import Node
from rclpy.action import ActionClient

from control_msgs.action import FollowJointTrajectory
from trajectory_msgs.msg import JointTrajectoryPoint
from rclpy.executors import  SingleThreadedExecutor
import threading
import json
import redis

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




class ROSBridge:
  
    def __init__(self):
        rclpy.init()
  
        self.joint_trajectory_client = URController()
        self.robot_joint_position = []
        self.executor1 = SingleThreadedExecutor()
       
        # # Add nodes to the executors
        self.executor1.add_node(self.joint_trajectory_client)
        # # # Start the executor in a separate thread
        self.executor_thread1 = threading.Thread(target=self.spin_executor1, daemon=True)     
        self.executor_thread1.start()
        
    def spin_executor1(self):
        try:
            self.executor1.spin()
           
        except KeyboardInterrupt:
            pass
        finally:
            self.executor1.shutdown()
           
    def publish_joint_angles(self, joint_positions):
        self.joint_trajectory_client.send_goal_sync(joint_positions)

def main(args=None):

    joint_positions = [
        1.47976908493042,   # shoulder_pan_joint
        -1.689188142816061,  # shoulder_lift_joint
        1.5098281065570276,  # elbow_joint
        -0.010845021610595751,  # wrist_1_joint
        1.440675139427185,   # wrist_2_joint
        1.690097689628601    # wrist_3_joint
    ]

    
    bridge = ROSBridge()
    bridge.publish_joint_angles(joint_positions)
    pass

if __name__ == '__main__':
    # main()
  
    
    REDIS_HOST = "172.17.0.1"  
    REDIS_PORT = 6379

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # Clear the queue before starting (to remove any message from previous sand_gym session)
    r.delete("joints_queue")
    bridge = ROSBridge()
    
    
    while True:
        # Block's until a message arrives on "task_queue"
        _, message = r.blpop("joints_queue")
        print(f"Received task: {message}")

        try:
            joint_positions = json.loads(message)
            bridge.publish_joint_angles(joint_positions)
            
            response = "successfully published joints"
                
        except (json.JSONDecodeError, TypeError):

            response = "Something wrong publishing joints"

        # Pushing the response to "joints_response_queue"
        r.lpush("joints_response_queue", response)
        