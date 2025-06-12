# ProgettoBigData
Stock Price Analisis

L'obiettivo di questo progetto è quello di andare a fare un'analisi dei dati storici di prezzi azionari e di fare un'analisi anche sulle informazioni aziendali che sono state estratte dal dataset [Daily historical stock prices](https://www.kaggle.com/datasets/ehallmar/daily-historical-stock-prices-1970-2018?select=historical_stocks.csv).

Queste analisi sono state fatte utilizzando una macchina virtuale linux in cui c'era installato la versione 24.04 di Ubuntu, e andando a sfruttare Hadoop MapReduce con input e output da HDFS per fare sia una fase iniziale di preprocessing che poi eseguire i job specifici per ottenere i dati d'interesse.

Inizialmente è stata configurata la macchina virtuale e installato Apache Hadoop nella versione 3.3.6 e ad installare java 8 che verra poi utilizzato per il codice dei job da eseguire. Hadoop è stato installato in modalità pseudo-distribuita in cui abbiamo namenode e datanode sulla stessa macchina, è stato creato un utente dedicato chiamato "hadoop" e poi dopo averlo abilitato è stata configurata SSH senza password con i seguenti comandi:

sudo adduser hadoop
sudo usermod -aG sudo hadoop
sudo -i -u hadoop
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ssh localhost

Successivamente, dopo l'installazione di Hadoop sono state impostate le seguenti variabili in ~/.bashrc:

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

e poi sono stati modificati i file core-site.xml aggiungendo:

```xml
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
</configuration>
```

hdfs-site.xml aggiungendo:
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///home/hadoop/hadoopdata/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///home/hadoop/hadoopdata/hdfs/datanode</value>
    </property>
 </configuration>
 ```

 mapred-site.xml aggiungendo:
```xml
 <configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
</configuration>
```

e yarn-site.xml aggiungendo:
```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
    </property>
</configuration>
```

Finita la configurazione della macchina virtuale sono quindi andato a sviluppare il codice java per andare ad eseguire una fase iniziale di preprocessing dei dati in cui attraverso l'utilizzo di un job MapReduce si vanno ad unire i dataset forniti in uno singolo che avrà le colonne ticker, name, date, close, low, high, volume, sector. Questi dati ottenuti dal preprocessing verranno poi sfruttati nei job successivi per andare a valutare volatilità, trend settoriale e fare un analisi per singolo titolo. I dati poi possono essere mostrati attraverso grafici o heatmap con degli script in Python, dopo essere stati convertiti da file .txt in file csv sempre attraverso l'ausilio di codice python.