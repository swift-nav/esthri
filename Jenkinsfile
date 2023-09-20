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
    stage('Prepare docker') {
      parallel {
        stage('Prepare docker (Rust latest)') {
          agent { dockerfile { reuseNode true } }
          steps {
            sh("echo done")
          }
        }
        stage('Prepare docker (Rust MSRV)') {
          agent { dockerfile { reuseNode true; additionalBuildArgs "--build-arg=RUST_VERSION=1.72.0"} }
          steps {
            sh("echo done")
          }
        }
      }
    }
    stage('Build checks') {
      parallel {
        stage('Build (rustls)') {
          agent { dockerfile { reuseNode true } }
          steps {
            sh("cargo make build")
          }
        }
        stage('Build library (rustls)') {
          agent { dockerfile { reuseNode true } }
          steps {
            sh("cargo make build-lib")
          }
        }
        stage('Build MSRV (rustls)') {
          agent { dockerfile { reuseNode true; additionalBuildArgs "--build-arg=RUST_VERSION=1.72.0"} }
          steps {
            sh("cargo make -p dev+msrv build-lib")
          }
        }
        stage('Build MSRV (nativetls)') {
          agent { dockerfile { reuseNode true; additionalBuildArgs "--build-arg=RUST_VERSION=1.72.0"} }
          steps {
            sh("cargo make -p dev+msrv+nativetls build-lib")
          }
        }
        stage('Build CLI (rustls)') {
          agent { dockerfile { reuseNode true } }
          steps {
            sh("cargo make build-cli")
          }
        }
        stage('Build (nativetls)') {
          agent { dockerfile { reuseNode true } }
          steps {
            sh("cargo make --profile dev+nativetls build")
          }
        }
        stage('Build library (nativetls)') {
          agent { dockerfile { reuseNode true } }
          steps {
            sh("cargo make --profile dev+nativetls build-lib")
          }
        }
        stage('Test (rustls)') {
          agent { dockerfile { reuseNode true; args dockerRunArgs } }
          environment {
            AWS_REGION = "us-west-2"
            AWS_DEFAULT_REGION = "us-west-2"
            USER = "jenkins"
          }
          steps {
            gitPrep()
            lock(resource: "esthri-integration-tests") {
              script {
                sh("""/bin/bash -ex
                    |
                    | git lfs pull
                    |
                    | cargo make test
                    |
                   """.stripMargin())
              }
            }
          }
        }
        stage('Test - minimum features (rustls)') {
          agent { dockerfile { reuseNode true; args dockerRunArgs } }
          environment {
            AWS_REGION = "us-west-2"
            AWS_DEFAULT_REGION = "us-west-2"
            USER = "jenkins"
          }
          steps {
            gitPrep()
            lock(resource: "esthri-integration-tests") {
              script {
                sh("""/bin/bash -ex
                    |
                    | git lfs pull
                    |
                    | cargo make test-min
                    |
                   """.stripMargin())
              }
            }
          }
        }
        stage('Test (nativetls)') {
          agent { dockerfile { reuseNode true; args dockerRunArgs } }
          environment {
            AWS_REGION = "us-west-2"
            AWS_DEFAULT_REGION = "us-west-2"
            USER = "jenkins"
          }
          steps {
            gitPrep()
            lock(resource: "esthri-integration-tests") {
              script {
                sh("""/bin/bash -ex
                    |
                    | git lfs pull
                    |
                    | cargo make --profile dev+nativetls test
                    |
                   """.stripMargin())
              }
            }
          }
        }
        stage('Test - minimum features (nativetls)') {
          agent { dockerfile { reuseNode true; args dockerRunArgs } }
          environment {
            AWS_REGION = "us-west-2"
            AWS_DEFAULT_REGION = "us-west-2"
            USER = "jenkins"
          }
          steps {
            gitPrep()
            lock(resource: "esthri-integration-tests") {
              script {
                sh("""/bin/bash -ex
                    |
                    | git lfs pull
                    |
                    | cargo make --profile dev+nativetls test-min
                    |
                   """.stripMargin())
              }
            }
          }
        }
        stage('Lint (rustls)') {
          agent { dockerfile { reuseNode true } }
          steps {
            script {
              sh("cargo make lint")
            }
          }
        }
        stage('Lint (nativetls)') {
          agent { dockerfile { reuseNode true } }
          steps {
            script {
              sh("cargo make --profile dev+nativetls lint")
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
  }
}
