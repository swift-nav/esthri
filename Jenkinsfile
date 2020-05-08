@Library("ci-jenkins") import com.swiftnav.ci.*

def context = new Context(context: this)
context.setRepo('esthri')

pipeline {
  agent {
    node {
      label('docker.m')
    }
  }
  environment {
    SCCACHE_BUCKET="sccache-linux"
    SCCACHE_SIZE="100G"
    SCCACHE_DIR="/opt/sccache"
    SCCACHE_REGION="us-west-2"
  }
  options {
    timeout(time: 1, unit: 'HOURS')
    timestamps()
    // Keep builds for 30 days.
    buildDiscarder(logRotator(daysToKeepStr: '30'))
  }
  stages {
    stage('Build checks') {
      parallel {
        stage('Build (release)') {
          agent { dockerfile { reuseNode true } }
          steps {
            sh("cargo build --release")
          }
        }
        stage('Test') {
          agent { dockerfile { reuseNode true } }
          environment {
            AWS_REGION = "us-west-2"
            AWS_DEFAULT_REGION = "us-west-2"
          }
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
              sh("cargo clippy --all-targets --features aggressive_lint")
            }
          }
        }
        stage('Format') {
          agent { dockerfile { reuseNode true } }
          steps {
            script {
              sh("cargo fmt -- --check")
            }
          }
        }
      }
    }
    stage('Publish release') {
      when {
        expression {
          context.isTagPush()
        }
      }
      environment {
        GITHUB_USER = "swiftnav-svc-jenkins"
      }
      steps {
        withCredentials([string(credentialsId: 'github-access-token-secretText', variable: 'GITHUB_TOKEN')]) {
          sh("""/bin/bash -ex
              | ./third_party/github-release-api/github_release_manager.sh \\
              | -l \$GITHUB_USER -t \$GITHUB_TOKEN \\
              | -o swift-nav -r ${context.repo}  \\
              | -d ${TAG_NAME} \\
              | -c upload target/release/esthri
              |""".stripMargin())
        }
      }
    }
  }
}
