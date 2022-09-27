library(tidyverse)

install.packages("RCurl")
library(RCurl)

iris <- read.csv(text = getURL('https://raw.githubusercontent.com/dataprofessor/data/master/iris.csv'))

summary(iris)

#summary of lm

iris %>% 
  lm(Sepal.Length ~ Petal.Width, data = .) %>% 
  summary()

#asigment lm to mod

mod <- lm(Sepal.Length ~ Petal.Width, data = iris)
plot(mod)

#plotting residuals

hist(mod$residuals)

#making a poredict

new_petal_width <- data.frame(Petal.Width = c(5,7,9,3))

predict(mod,new_petal_width) %>% 
  round(2)

#ploting everything

iris %>% 
  ggplot(aes(x=Sepal.Length, y= Petal.Width))+
  geom_point()+
  stat_smooth(method = 'lm')
