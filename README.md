# ros2_ur5e_bridge

## Dependencies

- Checking if redis running : `docker ps | grep redis`
- Running REDIS container : `docker run --rm -d --name redis -p 6379:6379 redis`
- Confirming its running fine : `docker exec -it redis redis-cli ping  # Should return "PONG"`

In the python environment/docker container you intend to run the bridge
```
pip3 install redis
```

## Execution 

```
python3 ros_bridge.py
```


