pipeline {
    agent {
        docker { image 'python:3.6' }
    }
    stages {
        stage('Test') {
            steps {
                sh 'echo $USER; echo $PWD; ls; pip install pytest'
                sh 'python setup.py install'
                sh 'pytest -s ./tst'
            }
        }
    }
}
