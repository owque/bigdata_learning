use owen;

create table t_user(
`userid` int ,
  `sex` string ,
  `age` int ,
  `occupation` int ,
  `zipcode` int
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ('field.delim'='::')
LOCATION '/owen/hive/data/user/';


create table t_movie
(
movieid bigint,
moviename string,
movietype string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ('field.delim'='::')
LOCATION '/owen/hive/data/movie';


create table t_rating
(
userid bigint,
movieid bigint,
rate double,
times  string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ('field.delim'='::')
LOCATION '/owen/hive/data/rating';


--=======================test1======================================
select b.Age, avg(a.Rate) avgrate
from t_rating a
join t_user b on a.UserID=b.UserID
where a.MovieID=2116
group by b.Age;

--result:
--1	3.2941176470588234
--18	3.3580246913580245
--25	3.436548223350254
--35	3.2278481012658227
--45	2.8275862068965516
--50	3.32
--56	3.5

--=======================test2======================================
select c.MovieName, avg(a.Rate) avgrate, count(1) total
from t_rating a
join t_user b on a.UserID = b.UserID
join t_movie c on a.MovieID=c.MovieID
where b.Sex='M'
group by c.MovieName
having total>50
sort by avgrate desc
limit 10;

--result:
--Sanjuro (1962)	4.639344262295082	61
--Godfather, The (1972)	4.583333333333333	1740
--Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954)	4.576628352490421	522
--Shawshank Redemption, The (1994)	4.560625	1600
--Raiders of the Lost Ark (1981)	4.520597322348094	1942
--Usual Suspects, The (1995)	4.518248175182482	1370
--Star Wars: Episode IV - A New Hope (1977)	4.495307167235495	2344
--Schindler's List (1993)	4.49141503848431	1689
--Paths of Glory (1957)	4.485148514851486	202
--Wrong Trousers, The (1993)	4.478260869565218	644

--=======================test3======================================
--=====step1:
select a.UserID, count(1) cnt
from t_rating a
join t_user b on a.UserID = b.UserID
where b.Sex='F'
group by a.UserID
sort by cnt desc limit 1;

--result: 1150


--=====step2:
select a.MovieID from t_rating a
where a.UserID =1150
order by a.rate desc limit 10

--result: 745,750,904,905,1094,1236,1256,1279,2064,2997

--=====step3:
select c.MovieName,avg(b.rate) avgrate from t_rating b
join t_movie c on b.MovieID=c.MovieID
where b.MovieID in
(745,750,904,905,1094,1236,1256,1279,2064,2997)
group by c.MovieName
order by avgrate desc


--result:
--Close Shave, A (1995)	4.52054794520548
--Rear Window (1954)	4.476190476190476
--Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1963)	4.4498902706656915
--It Happened One Night (1934)	4.280748663101604
--Duck Soup (1933)	4.21043771043771
--Trust (1990)	4.188888888888889
--Being John Malkovich (1999)	4.125390450691656
--Roger & Me (1989)	4.0739348370927315
--Night on Earth (1991)	3.747422680412371
--Crying Game, The (1992)	3.7314890154597236


--?????????????????????第三题，没想通下面的写法出来的结果为什么不对，
--去掉`group by y.MovieName`电影还是对的，加上之后变成的其他的电影。
--测试下来，只要使用聚合函数就不对，比如 distinct，collect_set
with t1 as(
select a.UserID, count(1) cnt
from t_rating a
join t_user b on a.UserID = b.UserID
where b.Sex='F'
group by a.UserID
sort by cnt desc limit 1
),
t2 as(
select aaa.MovieID,aaa.rate from t_rating aaa
where aaa.UserID in(select UserID from t1)
sort by rate desc limit 10
)
select y.MovieName,avg(x.rate) from t_rating x
join t_movie y on y.MovieID=x.MovieID
where x.MovieID in(select t2.MovieID from t2)
group by y.MovieName

--result:
--Badlands (1973)	4.078838174273859
--Big Lebowski, The (1998)	3.7383773928896993
--House of Yes, The (1997)	3.4742268041237114
--Rear Window (1954)	4.476190476190476
--Star Wars: Episode IV - A New Hope (1977)	4.453694416583082
--Fast, Cheap & Out of Control (1997)	3.8518518518518516
--Sound of Music, The (1965)	3.931972789115646
--City of Lost Children, The (1995)	4.062034739454094
--Roger & Me (1989)	4.0739348370927315
--Waiting for Guffman (1996)	4.147186147186147

-- 去掉group by
with t1 as(
select a.UserID, count(1) cnt
from t_rating a
join t_user b on a.UserID = b.UserID
where b.Sex='F'
group by a.UserID
sort by cnt desc limit 1
),
t2 as(
select aaa.MovieID,aaa.rate from t_rating aaa
where aaa.UserID in(select UserID from t1)
sort by rate desc limit 10
)
select y.MovieName from t_rating x
join t_movie y on y.MovieID=x.MovieID
where x.MovieID in(select t2.MovieID from t2)

--part of result:
--Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1963)
--Crying Game, The (1992)
--Being John Malkovich (1999)
--Crying Game, The (1992)
--Crying Game, The (1992)
--Close Shave, A (1995)
--Crying Game, The (1992)
--Trust (1990)
--Being John Malkovich (1999)
--Being John Malkovich (1999)
--Being John Malkovich (1999)
--It Happened One Night (1934)
--Being John Malkovich (1999)
--Crying Game, The (1992)
--Being John Malkovich (1999)
--It Happened One Night (1934)
--Duck Soup (1933)
--Duck Soup (1933)
--Being John Malkovich (1999)
--Duck Soup (1933)
--Being John Malkovich (1999)
--Close Shave, A (1995)
--Being John Malkovich (1999)
--Duck Soup (1933)
--Crying Game, The (1992)
--Rear Window (1954)
--It Happened One Night (1934)
--Being John Malkovich (1999)
--Being John Malkovich (1999)
--Being John Malkovich (1999)
--Close Shave, A (1995)


-- 最后发现这个用户评分最高的是5分，而他评5分的电影一共有59部，所以这可能是导致结果不同的原因，
-- 个人理解是不是加了聚合函数导致执行时发生了顺序上的变化。