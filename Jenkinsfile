@Library("ci-jenkins") import com.swiftnav.ci.*

def context = new Context(context: this)
context.setRepo('esthri')

String dockerRunArgs = "-e USER=jenkins --group-add staff"

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
            sh("cargo build --features http_server --release")
          }
        }
        stage('Test') {
          agent { dockerfile { reuseNode true; args dockerRunArgs } }
          environment {
            AWS_REGION = "us-west-2"
            AWS_DEFAULT_REGION = "us-west-2"
            USER = "jenkins"
          }
          steps {
            gitPrep()
            script {
              sh("""/bin/bash -ex
                  |
                  | git lfs install
                  | git lfs pull
                  |
                  | cargo test --features http_server -- --nocapture
                  |
                 """.stripMargin())
            }
          }
        }
        stage('Lint') {
          agent { dockerfile { reuseNode true } }
          steps {
            script {
              sh("cargo clippy --all-targets --features aggressive_lint,http_server")
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
        gitPrep()
        script {
          withCredentials([string(credentialsId: 'github-access-token-secretText', variable: 'GITHUB_TOKEN')]) {
            sh("""/bin/bash -ex
                |
                | ./third_party/github-release-api/github_release_manager.sh \\
                | -l \$GITHUB_USER -t \$GITHUB_TOKEN \\
                | -o swift-nav -r ${context.repo}  \\
                | -m "Release ${TAG_NAME}" \\
                | -d ${TAG_NAME} \\
                | -c create || :
                |
                | dir_name=esthri-${TAG_NAME}-linux_x86_64
                | archive_name=\${dir_name}.tgz
                |
                | mkdir \$dir_name
                |
                | strip target/release/esthri
                | cp target/release/esthri \$dir_name
                | cp bin/aws.esthri \$dir_name
                |
                | tar -cvzf \$archive_name \$dir_name
                |
                | ./third_party/github-release-api/github_release_manager.sh \\
                | -l \$GITHUB_USER -t \$GITHUB_TOKEN \\
                | -o swift-nav -r ${context.repo}  \\
                | -d ${TAG_NAME} \\
                | -c upload \$archive_name
                |
               """.stripMargin())
          }
        }
      }
    }
  }
}
