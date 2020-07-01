set -x
hdfs dfs -mkdir /data
hdfs dfs -mkdir /data/accounts
hdfs dfs -mkdir /data/orders
hdfs dfs -mkdir /data/cards
hdfs dfs -mkdir /data/loanhistories
hdfs dfs -mkdir /data/loans
hdfs dfs -mkdir /data/persons
hdfs dfs -mkdir /data/works


hdfs dfs -put crm/accounts.tsv /data/accounts
hdfs dfs -put crm/borroworders.tsv /data/orders
hdfs dfs -put crm/cards.tsv /data/cards
hdfs dfs -put crm/loanapprovalhistories.tsv /data/loanhistories
hdfs dfs -put crm/loans.tsv /data/loans
hdfs dfs -put crm/persons.tsv /data/persons
hdfs dfs -put crm/works.tsv /data/works