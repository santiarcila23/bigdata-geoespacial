// Pipeline CI/CD del sistema geoespacial.
// Se dispara con el webhook de GitHub (push a main) o, como respaldo, por polling cada 2 min.
//
// Checkout -> Build -> Pruebas unitarias -> Servicios -> Ingesta* -> Spark* -> Staging
//          -> Pruebas de API -> Despliegue
// (*) solo si faltan datos o se pide con los parámetros.
// Si cualquier etapa falla, la versión nueva NO se despliega: la API de producción
// sigue con la imagen anterior.

pipeline {
  agent any

  options {
    timestamps()
    disableConcurrentBuilds()
    skipDefaultCheckout(true)
    timeout(time: 90, unit: 'MINUTES')
    buildDiscarder(logRotator(numToKeepStr: '20'))
  }

  parameters {
    booleanParam(name: 'FORCE_INGEST', defaultValue: false,
                 description: 'Volver a descargar de Kaggle y recargar MongoDB')
    booleanParam(name: 'FORCE_SPARK', defaultValue: false,
                 description: 'Recalcular las agregaciones de Spark')
  }

  environment {
    COMPOSE = 'docker compose --profile ci'
  }

  stages {
    stage('Checkout') {
      steps {
        script {
          def scmVars = checkout scm
          env.APP_VERSION = "${scmVars.GIT_COMMIT.take(7)}-b${env.BUILD_NUMBER}"
          currentBuild.displayName = "#${env.BUILD_NUMBER} ${scmVars.GIT_COMMIT.take(7)}"
        }
        sh 'git log -1 --pretty="%h %an: %s"'
      }
    }

    stage('Build de imágenes') {
      steps {
        sh '$COMPOSE build api-staging ingest dask-scheduler dask-worker spark-master spark-worker spark-job'
      }
    }

    stage('Pruebas unitarias (pytest)') {
      steps {
        sh '''
          mkdir -p reports
          NAME="geo-unit-${BUILD_NUMBER}"
          set +e
          $COMPOSE run --name "$NAME" --no-deps api-staging \
            pytest tests/unit -q --junitxml=/tmp/unit.xml
          STATUS=$?
          set -e
          docker cp "$NAME:/tmp/unit.xml" reports/unit.xml || true
          docker rm -f "$NAME" >/dev/null 2>&1 || true
          exit $STATUS
        '''
      }
    }

    stage('Levantar servicios') {
      steps {
        sh '$COMPOSE up -d --wait mongo dask-scheduler dask-worker spark-master spark-worker'
        sh '$COMPOSE ps'
      }
    }

    stage('Ingesta: Kaggle + Dask -> MongoDB') {
      when {
        anyOf {
          expression { params.FORCE_INGEST }
          expression { countDocs('crimes') == 0 }
        }
      }
      steps {
        withCredentials([usernamePassword(credentialsId: 'kaggle-api',
                                          usernameVariable: 'KAGGLE_USERNAME',
                                          passwordVariable: 'KAGGLE_KEY')]) {
          // comillas simples: el secreto lo expande el shell, no Groovy (no aparece en el log)
          sh '$COMPOSE run --rm --no-deps -e KAGGLE_USERNAME -e KAGGLE_KEY -e FORCE_INGEST=$FORCE_INGEST ingest'
        }
      }
    }

    stage('Procesamiento: Spark -> colecciones agregadas') {
      steps {
        sh '$COMPOSE run --rm --no-deps -e FORCE_SPARK=$FORCE_SPARK spark-job'
      }
    }

    stage('Staging') {
      steps {
        sh '$COMPOSE up -d --no-deps --force-recreate --wait api-staging'
      }
    }

    stage('Pruebas de API (smoke)') {
      steps {
        sh '''
          NAME="geo-smoke-${BUILD_NUMBER}"
          set +e
          $COMPOSE run --name "$NAME" --no-deps -e API_URL=http://api-staging:5000 api-staging \
            pytest tests/smoke -q --junitxml=/tmp/smoke.xml
          STATUS=$?
          set -e
          docker cp "$NAME:/tmp/smoke.xml" reports/smoke.xml || true
          docker rm -f "$NAME" >/dev/null 2>&1 || true
          exit $STATUS
        '''
      }
    }

    stage('Despliegue') {
      steps {
        // Se promueve exactamente la imagen que pasó las pruebas
        sh 'docker tag geo-api:candidate geo-api:latest'
        sh '$COMPOSE up -d --no-deps --no-build --force-recreate --wait api'
        sh '''
          $COMPOSE exec -T api python -c "import json,urllib.request; \
            h=json.load(urllib.request.urlopen('http://localhost:5000/health')); \
            print(h); assert h['version']=='$APP_VERSION', 'version incorrecta'"
        '''
        echo "Versión ${env.APP_VERSION} desplegada en http://localhost:5000"
      }
    }
  }

  post {
    always {
      junit allowEmptyResults: true, testResults: 'reports/*.xml'
      sh '$COMPOSE stop api-staging || true'
    }
    success {
      echo 'Pipeline exitoso: nueva versión en producción.'
    }
    failure {
      echo 'Pipeline fallido: NO se desplegó. Producción sigue con la versión anterior.'
    }
  }
}

// Número de documentos de una colección (0 si MongoDB no responde)
def countDocs(String coll) {
  def out = sh(returnStdout: true, script: """
    \$COMPOSE exec -T mongo mongosh geo --quiet --eval 'db.${coll}.estimatedDocumentCount()' || echo 0
  """).trim()
  def lines = out.readLines()
  return lines ? (lines[-1].isInteger() ? lines[-1].toInteger() : 0) : 0
}
