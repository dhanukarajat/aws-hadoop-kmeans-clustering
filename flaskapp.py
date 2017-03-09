__author__ = 'Rajat'
from flask import Flask, redirect, render_template, request, session, send_from_directory, url_for, request
from random import randint
from werkzeug.utils import secure_filename
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import random
import sys
import boto.s3
import datetime
import urllib
import csv
import os
import subprocess
import tempfile
import pandas as pd
from sklearn.cluster import KMeans
from pandas import Series, DataFrame
import pygal

reload(sys)  
sys.setdefaultencoding('Cp1252')
global filename

application = Flask(__name__)
AWS_ACCESS_KEY_ID = "Enter_Access_key_id"
AWS_SECRET_KEY = "Enter_Secret_Key"

#Connecting to Amazon S3 bucket
conns3 = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_KEY)
bucket_name = "noname42"
bucket = conns3.create_bucket(bucket_name)


@application.route('/')
def init_aws():
    return render_template("index.html")

#List the contents of Amazon S3
@application.route('/list',methods=['GET','POST'])
def list():
    t1 = datetime.datetime.now()
    bucket = conns3.get_bucket(bucket_name)
    listOfFile = ""
    for key in bucket.list():
        listOfFile = listOfFile + "<li>" + key.name + "</li><br>"
    t2 = datetime.datetime.now()
    return listOfFile + "<br><br><i>Time taken for this operation : " + str(t2-t1) + "</i>"

#Upload file to Amazon S3
@application.route('/upload',methods=['GET','POST'])
def upload():
    if request.method == 'POST':
        file = request.files['file_upload']
        filename = file.filename
        contents = file.read()
        temp_dir = tempfile.mkdtemp()
        file_to_up = open(os.path.join(temp_dir, filename), 'w')
        file_to_up.write(contents)

        with open(os.path.join(temp_dir, filename), 'r') as localfile:
            bucket_to_upload_to = conns3.get_bucket(bucket_name)
            t1 = datetime.datetime.now()
            k = Key(bucket_to_upload_to)
            k.key = filename
            k.set_contents_from_file(localfile)
            t2 = datetime.datetime.now()
    return "<h1>Upload Successful!</h1>" + "<br><br><i>Time taken for this operation : " + str(t2-t1) + "</i>"

#Running Hadoop on Amazon EC2 instance containing sample file titanic.csv using mapper.py and reducer.py
@application.route('/hadoop',methods=['GET','POST'])
def hadoopa():
	num_mapper = request.form['num_mapper']
    num_reducer = request.form['num_reducer']
    p = subprocess.Popen(["hdfs", "dfs", "-copyFromLocal", "~/titanic1.csv", "/quiz1"], stdout=subprocess.PIPE)
    (output, err) = p.communicate()
    print "Uploading file to HDFS", output
    t1 = datetime.datetime.now()
    p = subprocess.Popen(["hadoop", "jar", "/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar", "-D mapred.map.tasks=num_mapper", "-D mapred.reduce.tasks=num_reducer", "-file", "/home/ubuntu/mapper.py", "-mapper", "/home/ubuntu/mapper.py", "-file", "/home/ubuntu/reducer.py", "-reducer", "/home/ubuntu/reducer.py", "-input", "/quiz/*", "-output", "/quiz/output1"], stdout=subprocess.PIPE)
    (output, err) = p.communicate()
    t2 = datetime.datetime.now()
    print "Running the file on hadoop" + output
    p = subprocess.call(["hdfs", "dfs", "-cat", "/quiz/output1/part-00000", ">", "/home/ubuntu/life1.txt"])

    return "<h1>Hadoop execution Successful!</h1><br>" + output + "<br><br><i>Time taken for this operation : " + str(t2-t1) + "</i>"

#Running k-means clustering and Visualization 
@application.route('/cluster', methods=['GET', 'POST'])
def clustering():
    file_to_upload = request.files['file_upload']
    file_name = file_to_upload.filename
    content = file_to_upload.read()
    col1name = str(request.form['col1'])

    
    col2name = str(request.form['col2'])
    number_of_cluster = int(request.form['number_of_cluster'])
    starttime = datetime.datetime.now()
    
    fo = open(file_name, "w")
    fo.write(content)
    
    k = Key(bucket)
    k.key = file_name
    starttime = datetime.datetime.now()
    k.set_contents_from_filename(secure_filename(file_name))
    endtime = datetime.datetime.now()
    resS3 = endtime - starttime
    
    file = open(file_to_upload.filename)
    # Read in the csv file
    df = pd.read_csv(file)
    # Retreive the dimension of the csv table
    print(df.shape)
    
    # Creating a dataframe
    df2 = DataFrame(df, columns=([col1name, col2name]))
    print(df2)
    
    # Generating clusters
    clusterModel = KMeans(n_clusters=number_of_cluster, random_state=1).fit(df2)
    labels = clusterModel.labels_
    centroids = clusterModel.cluster_centers_
    inertia = clusterModel.inertia_
    print('Labels', labels)
    print('Cluster Centers', centroids)
    print('Inertia', inertia)
    print(len(labels))
    
    final_dict = dict()
    for cluster_number in range(0, number_of_cluster):
        final_dict[cluster_number] = []
    print final_dict
    
    for i in range(0, len(labels)):
        try:
            dict_of_cordinates = df2.ix[i].to_dict()
        except:
            continue
        final_dict[labels[i]].append([dict_of_cordinates[col1name], dict_of_cordinates[col2name]])
    print final_dict
    
    xy_chart = pygal.XY(stroke=False)
    xy_chart.title = 'Propotionality between Latitude and Longitude'
    
    for i in range(0, len(labels)):
        xy_chart.add(i, final_dict[labels[i]])
    
    graph = xy_chart.render_data_uri()
    # xy_chart.render_to_file('chart.svg')
    return render_template('chart.html', chart=graph)	
	
port = os.getenv('PORT', '8000')
if __name__ == "__main__":
    application.debug = True
    application.run(host='0.0.0.0', port=int(port))