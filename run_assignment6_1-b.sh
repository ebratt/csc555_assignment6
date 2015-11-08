time hadoop jar MyJoinDriverPass1.jar /user/ec2-user/input/infected.txt /user/ec2-user/input/weblog.txt /user/ec2-user/output/join_output

time hadoop jar MyJoinDriverPass2.jar /user/ec2-user/output/join_output /user/ec2-user/output/join_output_final

hadoop fs -cat /user/ec2-user/output/join_output_final/part-r-00000
