pipeline {
    agent any
    stages {
        stage ('build') {
            steps {
                sh '''pip install -r requirements.txt'''
            }
        }
    }
    post {
        success {
            echo 'Requirements successfully installed'
        }
    }
}