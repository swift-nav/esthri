pipeline {
    agent {
        dockerfile true
    }

    options {
        timeout(time: 1, unit: 'HOURS')
        timestamps()
        // Keep builds for 30 days.
        buildDiscarder(logRotator(daysToKeepStr: '30'))
    }

    stages {
        stage('Build') {
            steps {
                sh("cargo build")
            }
        }
        stage('Test') {
            steps {
                sh("cargo test")
            }
        }
    }
}

