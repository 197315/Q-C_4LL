# Q-C_4LL
Data Quality rules library for pyspark context.


In the next table you can see the quality rules catalog:

| quality_rule | code_type | type | subtype | rule_description |
| :---: | :---: | :---: | :---: | :---: |
| R_1_1 	| 1 	| Basics 	| 1 	| Compara el data type esperado vs el data type real de la tabla. |
| R_1_2 	| 1 	| Basics 	| 2 	| Chequea valores Nulos. |
| R_1_3_D 	| 1 	| Basics 	| 3 	| Valores específicos no permitidos. (Discretos) |
| R_1_3_C 	| 1 	| Basics 	| 3 	| Valores no permitidos (rangos). |
| R_1_4 	| 1 	| Basics 	| 4 	| Valores dentro del rango esperado. |
| R_1_5 	| 1 	| Basics 	| 5 	| Valores dentro de un catálogo estático. |
| R_1_6 	| 1 	| Basics 	| 6 	| Comprueba que no existan caracteres no deseados. |
| R_2_1 	| 2 	| Users 	| 1 	| Chequea valores repetidos (duplicados, triplicados, etc.) |
| R_2_2 	| 2 	| Users 	| 2 	| Controlar la relacion entre variables de tablas distintas. |
| R_2_3 	| 2 	| Users 	| 3 	| Controlar la relacion entre variables de una misma tabla. |
| R_2_4 	| 2 	| Users 	| 4 	| Controlar que la agregación de una variable no supere determinado umbral. |


<br>__Code_type = 1__ are the basics rules
<br>__Code_type = 2__ are the users rules, it means that the user with his knowledge can apply the rules he needs.
