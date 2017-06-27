from flask import Flask,render_template, request
import os
from MRWordFrequency import MRWordFrequencyCount
from mrjob_1 import MRJobCount
import time
import getpass
import pandas as pd

app = Flask(__name__)

def get_data(file_name, col1,col2,col3):
    file_path = os.path.join(app.root_path,'datasets',file_name)
    data = pd.read_csv(file_path)

    data = data[[col1,col2,col3]]

    app.logger.error("User: {}".format(getpass.getuser()))
    cur_file_path = os.path.join(app.root_path, 'datasets', 'qdata.csv')

    data.to_csv(cur_file_path, sep=',', index=False, header=False)

    return cur_file_path

# Inital Setup run to check if the file is accessible and MRjob is working
@app.route('/',methods=['GET','POST'])
def runmr():
    if request.method == 'GET':
        return render_template('first_page.html')
    elif request.method == 'POST':
        data = {}
        file_path = os.path.join(app.root_path,'datasets','data.csv')

        mr_job = MRWordFrequencyCount(args=['-r','local', '--jobconf', 'mapred.map.tasks={}'.format(10),
                                            '--jobconf',' mapred.reduce.tasks={}'.format(2),'{}'.format(file_path)])
        with mr_job.make_runner() as runner:
            start_time = time.time()
            runner.run()
            end_time =time.time()

            for line in runner.stream_output():
                key,value = mr_job.parse_output_line(line)
                data[key] = value

        max_time = (end_time - start_time)

        proper_data = " <h2> Chars : {0} <h2> <h2> Words : {1} <h2> <h2> Lines : {2} <h2>".format(data['chars'], data['lines'], data['words'])
        return render_template('first_page.html', datainfo=proper_data, timeinfo=max_time)

# Query Processing
@app.route('/query1',methods=['GET','POST'])
def query_one():
    if request.method == 'GET':
        return render_template('query_1.html')
    elif request.method == 'POST':
        lower = request.form['age1']
        upper = request.form['age2']
        gender = request.form['gender']

        # file = open(os.path.join(app.root_path,'datasets','range.txt'),'r')
        data_dict = {}

        data_path = get_data('data.csv','Gender','Age','Kilograms')

        # ret_data = get_data('test.csv')

        mr_job = MRJobCount(args=['-r', 'local',
                                  '--jobconf', 'mapred.map.tasks={0}'.format(10),
                                  '--jobconf', 'mapred.reduce.tasks={0}'.format(2),
                                  '--jobconf', 'my.job.lower_age={0}'.format(lower),
                                  '--jobconf', 'my.job.upper_age={0}'.format(upper),
                                  '{}'.format(data_path)])

        with mr_job.make_runner() as runner:
            start_time = time.time()
            runner.run()
            end_time = time.time()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                data_dict[key] = value

        max_time = (end_time - start_time)

        return render_template('query_1.html', dataInfo=data_dict, timeinfo=max_time)

if __name__ == '__main__':
    app.run()
