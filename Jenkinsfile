pipeline {
    agent any
    stages {
        stage ('build') {
            steps {
                sh '''
                python3 -m venv env && source env/bin/activate
                pip install -r requirements.txt'''
            }
        }
    }
    post {
        always {
            sh '''rm -r env/'''
        }
        success {
            echo 'Requirements successfully installed'
        }
    }
}