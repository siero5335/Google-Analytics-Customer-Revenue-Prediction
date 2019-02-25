library("keras"); library("embed")

library(tidymodels)
library(AmesHousing)
ames <- make_ames()
length(levels(ames$Neighborhood))
set.seed(71)

use_backend(backend = "plaidml")

means <- 
  ames %>%
  group_by(Neighborhood) %>%
  summarise(
    mean = mean(log10(Sale_Price)),
    n = length(Sale_Price),
    lon = median(Longitude),
    lat = median(Latitude)
  )


plaid_embed <- 
  recipe(Sale_Price ~ ., data = ames) %>%
  step_log(Sale_Price, base = 10) %>%
  # Add some other predictors that can be used by the network. We
  # preprocess them first
  step_YeoJohnson(Lot_Area, Full_Bath, Gr_Liv_Area)  %>%
  step_range(Lot_Area, Full_Bath, Gr_Liv_Area)  %>%
  step_embed(
    Neighborhood, 
    outcome = vars(Sale_Price),
    predictors = vars(Lot_Area, Full_Bath, Gr_Liv_Area),
    num_terms = 5, 
    hidden_units = 10, 
    options = embed_control(epochs = 75, validation_split = 0.2)
  ) %>% 
  prep(training = ames)

plaid_embed$steps[[4]]$history %>%
  filter(epochs > 1) %>%
  ggplot(aes(x = epochs, y = loss, col = type)) + 
  geom_line()


hood_coef <- 
  tidy(plaid_embed, number = 4) %>%
  dplyr::select(-terms)  %>%
  dplyr::rename(Neighborhood = level) %>%
  # Make names smaller
  rename_at(vars(contains("emb")), funs(gsub("Neighborhood_", "", ., fixed = TRUE)))
hood_coef
