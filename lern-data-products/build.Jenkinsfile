node('build-slave') {
    try {
        String ANSI_GREEN = "\u001B[32m"
        String ANSI_NORMAL = "\u001B[0m"
        String ANSI_BOLD = "\u001B[1m"
        String ANSI_RED = "\u001B[31m"
        String ANSI_YELLOW = "\u001B[33m"
        ansiColor('xterm') {
            stage('Checkout') {
                cleanWs()
                checkout scm
                commit_hash = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
                artifact_version = sh(script: "echo " + params.github_release_tag.split('/')[-1] + "_" + commit_hash + "_" + env.BUILD_NUMBER, returnStdout: true).trim()
                echo "artifact_version: " + artifact_version
            }
        }
        stage('Pre-Build') {
            sh '''
                #sed -i "s/'replication_factor': '2'/'replication_factor': '1'/g" scripts/database/data.cql
                '''
        }
        stage('Build') {
            sh '''
                cd lern-data-products
                export JAVA_HOME=/usr/lib/jvm/jdk-11.0.2
                export PATH=$JAVA_HOME/bin:$PATH
                echo $(java -version)
                mvn clean install -DskipTests -DCLOUD_STORE_GROUP_ID=${params.cloud_store_group_id} -DCLOUD_STORE_ARTIFACT_ID=${params.cloud_store_artifact_id} -DCLOUD_STORE_VERSION=${params.cloud_store_version}
                '''
        }
        stage('Archive artifacts'){
            sh """
                mkdir lern_data_products_artifacts
                cp ./lern-data-products/target/lern-data-products-1.0-distribution.tar.gz lern_data_products_artifacts
                zip -j lern_data_products_artifacts.zip:${artifact_version} lern_data_products_artifacts/*
            """
            archiveArtifacts artifacts: "lern_data_products_artifacts.zip:${artifact_version}", fingerprint: true, onlyIfSuccessful: true
            sh """echo {\\"artifact_name\\" : \\"lern_data_products_artifacts.zip\\", \\"artifact_version\\" : \\"${artifact_version}\\", \\"node_name\\" : \\"${env.NODE_NAME}\\"} > metadata.json"""
            archiveArtifacts artifacts: 'metadata.json', onlyIfSuccessful: true
            currentBuild.description = artifact_version
        }
    }
    catch (err) {
        currentBuild.result = "FAILURE"
        throw err
    }
}