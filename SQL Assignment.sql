Data Scientist Role Play: Profiling and Analyzing the Yelp Dataset Coursera Worksheet

This is a 2-part assignment. In the first part, you are asked a series of questions that will help you profile and understand the data just like a data scientist would. For this first part of the assignment, you will be assessed both on the correctness of your findings, as well as the code you used to arrive at your answer. You will be graded on how easy your code is to read, so remember to use proper formatting and comments where necessary.

In the second part of the assignment, you are asked to come up with your own inferences and analysis of the data for a particular research question you want to answer. You will be required to prepare the dataset for the analysis you choose to do. As with the first part, you will be graded, in part, on how easy your code is to read, so use proper formatting and comments to illustrate and communicate your intent as required.

For both parts of this assignment, use this "worksheet." It provides all the questions you are being asked, and your job will be to transfer your answers and SQL coding where indicated into this worksheet so that your peers can review your work. You should be able to use any Text Editor (Windows Notepad, Apple TextEdit, Notepad ++, Sublime Text, etc.) to copy and paste your answers. If you are going to use Word or some other page layout application, just be careful to make sure your answers and code are lined appropriately.
In this case, you may want to save as a PDF to ensure your formatting remains intact for you reviewer.



                                                      Part 1: Yelp Dataset Profiling and Understanding



1. Profile the data by finding the total number of records for each of the tables below:
	
i. Attribute table = 10000
ii. Business table = 10000
iii. Category table = 10000
iv. Checkin table = 10000
v. elite_years table = 10000
vi. friend table = 10000
vii. hours table = 10000
viii. photo table = 10000
ix. review table = 10000
x. tip table = 10000
xi. user table = 10000
	


2. Find the total distinct records by either the foreign key or primary key for each table. If two foreign keys are listed in the table, please specify which foreign key.

i. Business =      key id 10000 records
ii. Hours =        foreign key business_id 1562 records
iii. Category =    foreign key business_id 2643 records
iv. Attribute =    foreign key business_id 1115 records
v. Review = 	   key id 10000, foreign key business_id 8090 records, foreign key user_id 9581 records
vi. Checkin = 	   foreign key business_id 493 records
vii. Photo = 	   key id 10000, foreign key business_id 6493 records
viii. Tip = 	   foreign key business_id 3979 records, foreign key user_id 537 records
ix. User = 	   key id 10000 records
x. Friend = 	   foreign key 11 records
xi. Elite_years =  foreign key user_id 2780 records

Note: Primary Keys are denoted in the ER-Diagram with a yellow key icon.	



3. Are there any columns with null values in the Users table? Indicate "yes," or "no."

	Answer: NO
	
	
	SQL code used to arrive at answer:

select 
case 
    when name is null then 1
    when review_count is null then 1
    when yelping_since is null then 1
    when useful is null then 1
    when funny is null then 1
    when cool is null then 1
    when fans is null then 1
    when average_stars is null then 1
    when compliment_hot is null then 1
    when compliment_more is null then 1
    when compliment_profile is null then 1
    when compliment_cute is null then 1
    when compliment_list is null then 1
    when compliment_note is null then 1
    when compliment_plain is null then 1
    when compliment_cool is null then 1
    when compliment_funny is null then 1
    when compliment_writer is null then 1
    when compliment_photos is null then 1
    else 0
end as col
from user
group by col
	
	

	
4. For each table and column listed below, display the smallest (minimum), largest (maximum), and average (mean) value for the following fields:

	i. Table: Review, Column: Stars
	
		min: 1		max: 5		avg: 3.7082
		
	
	ii. Table: Business, Column: Stars
	
		min: 1		max: 5		avg: 3.6549
		
	
	iii. Table: Tip, Column: Likes
	
		min: 0		max: 2		avg: 0.0144 
		
	
	iv. Table: Checkin, Column: Count
	
		min: 1		max: 53		avg: 1.9414
		
	
	v. Table: User, Column: Review_count
	
		min: 0		max: 2000		avg: 24.2995
	
	


5. List the cities with the most reviews in descending order:

	SQL code used to arrive at answer: 

select
city,
sum(review_count) as review_count
from business
group by city
order by review_count desc
	
	
	Copy and Paste the Result Below:
	

+-----------------+--------------+
| city            | review_count |
+-----------------+--------------+
| Las Vegas       |        82854 |
| Phoenix         |        34503 |
| Toronto         |        24113 |
| Scottsdale      |        20614 |
| Charlotte       |        12523 |
| Henderson       |        10871 |
| Tempe           |        10504 |
| Pittsburgh      |         9798 |
| Montr�al        |         9448 |
| Chandler        |         8112 |
| Mesa            |         6875 |
| Gilbert         |         6380 |
| Cleveland       |         5593 |
| Madison         |         5265 |
| Glendale        |         4406 |
| Mississauga     |         3814 |
| Edinburgh       |         2792 |
| Peoria          |         2624 |
| North Las Vegas |         2438 |
| Markham         |         2352 |
| Champaign       |         2029 |
| Stuttgart       |         1849 |
| Surprise        |         1520 |
| Lakewood        |         1465 |
| Goodyear        |         1155 |
+-----------------+--------------+
(Output limit exceeded, 25 of 362 total rows shown)




6. Find the distribution of star ratings to the business in the following cities:

i. Avon

SQL code used to arrive at answer:

select
stars as start_rating,
count(stars)
from business
where city = 'Avon'
group by stars

Copy and Paste the Resulting Table Below (2 columns – star rating and count):

+--------------+--------------+
| start_rating | count(stars) |
+--------------+--------------+
|          1.5 |            1 |
|          2.5 |            2 |
|          3.5 |            3 |
|          4.0 |            2 |
|          4.5 |            1 |
|          5.0 |            1 |
+--------------+--------------+




ii. Beachwood

SQL code used to arrive at answer:

select
stars as start_rating,
count(stars)
from business
where city = 'Beachwood'
group by stars

Copy and Paste the Resulting Table Below (2 columns – star rating and count):
		
+--------------+--------------+
| start_rating | count(stars) |
+--------------+--------------+
|          2.0 |            1 |
|          2.5 |            1 |
|          3.0 |            2 |
|          3.5 |            2 |
|          4.0 |            1 |
|          4.5 |            2 |
|          5.0 |            5 |
+--------------+--------------+




7. Find the top 3 users based on their total number of reviews:
		
	SQL code used to arrive at answer:

select 
id,
review_count
from user
order by review_count desc
limit 3
		
	Copy and Paste the Result Below:

+------------------------+--------------+
| id                     | review_count |
+------------------------+--------------+
| -G7Zkl1wIWBBmD0KRy_sCw |         2000 |
| -3s52C4zL_DHRK0ULG6qtg |         1629 |
| -8lbUNlXVSoXqaRRiHiSNg |         1339 |
+------------------------+--------------+		




8. Does posing more reviews correlate with more fans?
	
	no	

	Please explain your findings and interpretation of the results:
	
select name, review_count, fans
			from user
			order by fans desc
			limit 10
		
		Results:
			+-----------+--------------+------+
			| name      | review_count | fans |
			+-----------+--------------+------+
			| Amy       |          609 |  503 |
			| Mimi      |          968 |  497 |
			| Harald    |         1153 |  311 |
			| Gerald    |         2000 |  253 |
			| Christine |          930 |  173 |
			| Lisa      |          813 |  159 |
			| Cat       |          377 |  133 |
			| William   |         1215 |  126 |
			| Fran      |          862 |  124 |
			| Lissa     |          834 |  120 |
			+-----------+--------------+------+
	




9. Are there more reviews with the word "love" or with the word "hate" in them?

	Answer: love wins by 1780 vs 232 of hate

	
	SQL code used to arrive at answer:

select
sum(case 
    when text like '%love%' then 1
    else 0
end) as love,
sum(case 
    when text like '%hate%' then 1
    else 0
end) as hate
from review
	
+------+------+
| love | hate |
+------+------+
| 1780 |  232 |
+------+------+
	




10. Find the top 10 users with the most fans:

	SQL code used to arrive at answer:
	
select 
id,
fans
from user
order by fans desc
limit 10

	Copy and Paste the Result Below:

+------------------------+------+
| id                     | fans |
+------------------------+------+
| -9I98YbNQnLdAmcYfb324Q |  503 |
| -8EnCioUmDygAbsYZmTeRQ |  497 |
| --2vR0DIsmQ6WfcSzKWigw |  311 |
| -G7Zkl1wIWBBmD0KRy_sCw |  253 |
| -0IiMAZI2SsQ7VmyzJjokQ |  173 |
| -g3XIcCb2b-BD0QBCcq2Sw |  159 |
| -9bbDysuiWeo2VShFJJtcw |  133 |
| -FZBTkAZEXoP7CYvRV2ZwQ |  126 |
| -9da1xk7zgnnfO1uTVYGkA |  124 |
| -lh59ko3dxChBSZ9U7LfUw |  120 |
+------------------------+------+
		



                                                             Part 2: Inferences and Analysis



1. Pick one city and category of your choice and group the businesses in that city or category by their overall star rating. Compare the businesses with 2-3 stars to the businesses with 4-5 stars and answer the following questions. Include your code. ( city: Toronto, category: restorant)
	
i. Do the two groups you chose to analyze have a different distribution of hours?
		
	Yes but stars are not related with the hours.

select 
name,
hours,
case
    when stars between 2 and 3 then 'mid'
    when stars between 4 and 5 then 'good'
    else 'bad'
end stars
from ((business b left join category c
on b.id = c.business_id)left join hours h
on b.id = h.business_id)
where city like 'Toronto' and category like 'Restaurants'

+------------------+-----------------------+-------+
| name             |                 hours | stars |
+------------------+-----------------------+-------+
| Mama Mia         |                  None | good  |
| Cabin Fever      |     Monday|16:00-2:00 | good  |
| Cabin Fever      |    Tuesday|18:00-2:00 | good  |
| Cabin Fever      |     Friday|18:00-2:00 | good  |
| Cabin Fever      |  Wednesday|18:00-2:00 | good  |
| Cabin Fever      |   Thursday|18:00-2:00 | good  |
| Cabin Fever      |     Sunday|16:00-2:00 | good  |
| Cabin Fever      |   Saturday|16:00-2:00 | good  |
| Royal Dumpling   |                  None | bad   |
| Big Smoke Burger |    Monday|10:30-21:00 | mid   |
| Big Smoke Burger |   Tuesday|10:30-21:00 | mid   |
| Big Smoke Burger |    Friday|10:30-21:00 | mid   |
| Big Smoke Burger | Wednesday|10:30-21:00 | mid   |
| Big Smoke Burger |  Thursday|10:30-21:00 | mid   |
| Big Smoke Burger |    Sunday|11:00-19:00 | mid   |
| Big Smoke Burger |  Saturday|10:30-21:00 | mid   |
| Sushi Osaka      |    Monday|11:00-23:00 | good  |
| Sushi Osaka      |   Tuesday|11:00-23:00 | good  |
| Sushi Osaka      |    Friday|11:00-23:00 | good  |
| Sushi Osaka      | Wednesday|11:00-23:00 | good  |
| Sushi Osaka      |  Thursday|11:00-23:00 | good  |
| Sushi Osaka      |    Sunday|14:00-23:00 | good  |
| Sushi Osaka      |  Saturday|11:00-23:00 | good  |
| 99 Cent Sushi    |    Monday|11:00-23:00 | mid   |
| 99 Cent Sushi    |   Tuesday|11:00-23:00 | mid   |
+------------------+-----------------------+-------+
(Output limit exceeded, 25 of 44 total rows shown)




ii. Do the two groups you chose to analyze have a different number of reviews?

            yes

select 
name,
review_count,
case
    when stars between 2 and 3 then 'mid'
    when stars between 4 and 5 then 'good'
    else 'bad'
end stars
from business b left join category c
on b.id = c.business_id
where city like 'Toronto' and category like 'Restaurants'

+--------------------+--------------+-------+
| name               | review_count | stars |
+--------------------+--------------+-------+
| Mama Mia           |            8 | good  |
| Cabin Fever        |           26 | good  |
| Royal Dumpling     |            4 | bad   |
| Big Smoke Burger   |           47 | mid   |
| Sushi Osaka        |            8 | good  |
| 99 Cent Sushi      |            5 | mid   |
| Pizzaiolo          |           34 | mid   |
| Naniwa-Taro        |           75 | good  |
| The Kosher Gourmet |            3 | bad   |
| Edulis             |           89 | good  |
+--------------------+--------------+-------+





iii. Are you able to infer anything from the location data provided between these two groups? Explain.

no enough information to make a hypothesis

SQL code used for analysis:

select 
name,
neighborhood,
address,
case
    when stars between 2 and 3 then 'mid'
    when stars between 4 and 5 then 'good'
    else 'bad'
end stars
from business b left join category c
on b.id = c.business_id
where city like 'Toronto' and category like 'Restaurants'

+--------------------+------------------------+--------------------------+-------+
| name               | neighborhood           | address                  | stars |
+--------------------+------------------------+--------------------------+-------+
| Mama Mia           |                        | 816 Saint Clair Avenue W | good  |
| Cabin Fever        | High Park              | 1669 Bloor Street W      | good  |
| Royal Dumpling     | Willowdale             | 5 Northtown Way, Unit 7  | bad   |
| Big Smoke Burger   | Downtown Core          | 260 Yonge Street         | mid   |
| Sushi Osaka        | Etobicoke              | 5084 Dundas Street W     | good  |
| 99 Cent Sushi      | Downtown Core          | 389 Church Street        | mid   |
| Pizzaiolo          | Entertainment District | 270 Adelaide Street W    | mid   |
| Naniwa-Taro        | Willowdale             | 7 Byng Avenue            | good  |
| The Kosher Gourmet |                        | 3003 Bathurst Street     | bad   |
| Edulis             | Niagara                | 169 Niagara Street       | good  |
+--------------------+------------------------+--------------------------+-------+





		
2. Group business based on the ones that are open and the ones that are closed. What differences can you find between the ones that are still open and the ones that are closed? List at least two differences and the SQL code you used to arrive at your answer.
		
i. Difference 1:
         
	open has more stars rating than close
         
ii. Difference 2:
         
         open has more reviews than close
         
SQL code used for analysis:

select 
is_open,
b.stars,
b.review_count
from business b inner join category c
on b.id = c.business_id
where city like 'Toronto'  and category like 'Restaurants'
group by is_open

+---------+-------+--------------+
| is_open | stars | review_count |
+---------+-------+--------------+
|       0 |   2.0 |            5 |
|       1 |   4.0 |           89 |
+---------+-------+--------------+
	
	
3. For this last part of your analysis, you are going to choose the type of analysis you want to conduct on the Yelp dataset and are going to prepare the data for analysis.

Ideas for analysis include: Parsing out keywords and business attributes for sentiment analysis, clustering businesses to find commonalities or anomalies between them, predicting the overall star rating for a business, predicting the number of fans a user will have, and so on. These are just a few examples to get you started, so feel free to be creative and come up with your own problem you want to solve. Provide answers, in-line, to all of the following:
	
i. Indicate the type of analysis you chose to do:
         
	the more success category and bussiness rating by sentiment reviews
         
ii. Write 1-2 brief paragraphs on the type of data you will need for your analysis and why you chose that data:
             this dataset compiles business category and displays the user opinions ratings by numerical rating of reviews , stars, and othes sentiments like usefull coll and funny for each category...can be usefull to search the succesfull categories and see the ratio of sentiments that customers have from.              
                  
iii. Output of your finished dataset:

+---------------------------+----------------+-----------+-------------+-------------+-----------+------------+
|                  category | total_business | stars_avg | reviews_avg | useful_rate | cool_rate | funny_rate |
+---------------------------+----------------+-----------+-------------+-------------+-----------+------------+
|                      None |           9940 |      3.65 |       39.08 |        0.86 |      0.39 |       0.26 |
|               Restaurants |             75 |      3.49 |       84.29 |        0.89 |      0.22 |       0.11 |
|                  Shopping |             31 |       4.0 |       54.84 |         0.5 |       0.5 |        0.5 |
|                      Food |             26 |      3.83 |      129.46 |        0.67 |      0.33 |       0.17 |
|                 Nightlife |             22 |      3.52 |      100.59 |        0.25 |       0.0 |        0.0 |
|                      Bars |             19 |      3.55 |      114.95 |        0.33 |       0.0 |        0.0 |
|          Health & Medical |             17 |      4.09 |       11.94 |        None |      None |       None |
|             Home Services |             16 |       4.0 |        5.88 |        None |      None |       None |
|    American (Traditional) |             13 |      3.85 |      153.08 |        0.25 |       0.0 |        0.0 |
|             Beauty & Spas |             13 |      3.88 |        9.15 |        None |      None |       None |
|            Local Services |             12 |      4.21 |        8.33 |        None |      None |       None |
|               Active Life |             10 |      4.15 |        13.1 |         6.0 |       4.0 |        6.0 |
|                Automotive |              9 |       4.5 |        22.0 |        None |      None |       None |
|           Hotels & Travel |              9 |      3.22 |       42.33 |        None |      None |       None |
|                Sandwiches |              8 |      3.94 |      121.75 |         0.0 |       0.0 |        0.0 |
|                   Burgers |              8 |      3.13 |       37.13 |        None |      None |       None |
|                   Mexican |              7 |       3.5 |       46.71 |        None |      None |       None |
|      Arts & Entertainment |              7 |       4.0 |       55.43 |         0.0 |       0.0 |        0.0 |
|                 Fast Food |              7 |      3.21 |       26.43 |        None |      None |       None |
| Event Planning & Services |              6 |      3.75 |       19.67 |        None |      None |       None |
|               Hair Salons |              6 |      4.08 |       10.83 |        None |      None |       None |
|            Specialty Food |              6 |      4.08 |      269.83 |         0.5 |       0.5 |        0.5 |
|            American (New) |              6 |      3.33 |       80.17 |        None |      None |       None |
|                  Bakeries |              5 |       4.1 |        47.8 |        None |      None |       None |
|                  Japanese |              5 |       3.8 |        30.4 |        None |      None |       None |
+---------------------------+----------------+-----------+-------------+-------------+-----------+------------+
(Output limit exceeded, 25 of 258 total rows shown)      
         
iv. Provide the SQL code you used to create your final dataset:

select 
category,
count(b.id) total_business,
round(avg(b.stars),2) as stars_avg,
round(avg(b.review_count),2) as reviews_avg,
round(avg(useful),2) as useful_rate,
round(avg(cool),2) as cool_rate,
round(avg(funny),2) as funny_rate
from ((business b left join category c
    on b.id = c.business_id)left join review r 
    on b.id = r.business_id)
group by category
order by total_business desc, b.stars desc,b.review_count desc