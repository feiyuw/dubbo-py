pipeline {
    agent {
        docker {
            image 'registry.cn-hangzhou.aliyuncs.com/alpd/python:3.9.1'
        }
    }
    stages {
        stage('Test') {
            steps {
                sh 'python setup.py install'
                sh 'pytest -s ./tst'
            }
        }
    }
}
