time hadoop jar MyDriverPass1.jar /user/ec2-user/input/csc555_assign6_input.txt /user/ec2-user/output/csc555_assign6_output_pass1

time hadoop jar MyDriverPass2.jar /user/ec2-user/output/csc555_assign6_output_pass1/part-r-00000 /user/ec2-user/output/csc555_assign6_output_pass2

hadoop fs -cat /user/ec2-user/output/csc555_assign6_output_pass2/part-r-00000
