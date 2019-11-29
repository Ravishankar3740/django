if len(glob.glob("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+filename)) != 0:
    filename1 =r"/home/ubuntu/RuleBuilder/s3_fileupload/media/"+filename
    print("m in media")
    print(filename1)
elif len(glob.glob("/home/ubuntu/RuleBuilder/s3_fileupload/Merge/"+filename)) != 0:
    filename1 =r"/home/ubuntu/RuleBuilder/s3_fileupload/Merge/"+filename
elif len(glob.glob("/home/ubuntu/RuleBuilder/s3_fileupload/client_compute/"+filename)) != 0:
    filename1 =r"/home/ubuntu/RuleBuilder/s3_fileupload/client_compute/"+filename
elif len(glob.glob("/home/ubuntu/RuleBuilder/s3_fileupload/media/Master_Files/"+filename)) != 0:
    filename1 =r"/home/ubuntu/RuleBuilder/s3_fileupload/media/Master_Files/"+filename
else:
    df_data = 'File Corrupt'
    return df_data
