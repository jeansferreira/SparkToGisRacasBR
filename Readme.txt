Projeto para Carga de arquivos  CVS com dados de Latitude e Longitude de localização da população do Brasil.

	Intuito do projeto era inserir os dados com Spark e medir a velocidade de uso da biblioteca da apache com a Ferramenta de Integração da Pentaho PDI.
	
Observei que a biblioteca da apache Spark é extremamente muito mais rápida que o PDI 7. Com spark a velocidade da inserção dos dados na base de dados Postgres9.6 foi de 20 minutos para mais de 200milhões de registros, já com a ferramenta da Pentaho o mesmo tempo apenas inseriu 500mil registros.	