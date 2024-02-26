### TP NOTÉ BIG DATA 
## Justine Fouillé & Gabin Raapoto


Création d'un topic spark sur kafka via la ligne de commande.
`docker exec broker \                    
kafka-topics --bootstrap-server broker:9092 \     
--create \
--topic spark`




`docker exec --interactive --tty broker \
kafka-console-consumer --bootstrap-server broker:9092 \
--consumer-property group.id=app_test \
--topic IA_topic \
--from-beginning`

Pour visualiser que le producer fonctionne correctement on lit le topic. `