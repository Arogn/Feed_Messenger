# Feed_Messenger
DAG для Apache Airflow, который отправляет каждую утро аналитическую сводку сразу по двум приложениям "лента новостей" и "мессенджер"

Аналитическая сводка показывает следующие метрики:

- кол-во людей, которые пользовались только лентой новостей за последнюю неделю 
- кол-во людей, которые пользовались только мессенджером за последнюю неделю
- кол-во людей, которые пользовались и лентой новостей и мессенджером за последнюю неделю
- DAU за вчера по мессенджеру 
- DAU, просмотры, лайки, CTR за вчера по ленте новостей 
- графики DAU за последнюю неделю по мессенджеру 
- графики DAU, просмотры, лайки, CTR за последнюю неделю по ленте новостей 