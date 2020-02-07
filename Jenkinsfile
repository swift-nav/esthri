pipeline {
  agent {
    node {
      label('docker.m')
    }
  }
  options {
    timeout(time: 1, unit: 'HOURS')
    timestamps()
    // Keep builds for 30 days.
    buildDiscarder(logRotator(daysToKeepStr: '30'))
  }
  stages {
    stage('Build') {
      agent { dockerfile { reuseNode true } }
      steps {
        sh("cargo build")
      }
    }
    stage('Build checks') {
      parallel {
        stage('Test') {
          agent { dockerfile { reuseNode true } }
          steps {
            script {
              sh("cargo test")
            }
          }
        }
        stage('Lint') {
          agent { dockerfile { reuseNode true } }
          steps {
            script {
              sh("cargo clippy")
            }
          }
        }
        stage('Format') {
          agent { dockerfile { reuseNode true } }
          steps {
            script {
              sh("cargo format -- --check")
            }
          }
        }
      }
    }
  }
}
