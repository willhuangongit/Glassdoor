# ==========
# Load packages
library(gridExtra)
library(grid)
library(rstudioapi)
library(ggplot2)
library(ggpubr)
library(gtable)
library(RMySQL)

# ==========
# Parameters

comp_id = "6036"

# Colour palette
# This mimics Glassdoor's colour palette

green_vec = c(12, 170, 65) / 255
yellow_vec = c(246, 181, 0) / 255
red_vec = c(235, 65, 51) / 255
light_grey_vec = c(234, 234, 234) / 255

green = do.call("rgb", as.list(green_vec))
yellow = do.call("rgb", as.list(yellow_vec))
red = do.call("rgb", as.list(red_vec))
light_grey = do.call("rgb", as.list(light_grey_vec))



# Functions

reverse <- function(x){
  y <- c()
  
}

cond_changes <- function(x, from, to){
  for(i in 1:length(x)){
    if(x[i] %in% from){
      j <- match(x[i], from)
      x[i] <- to[j]
    }
  }
  return(x)
}


freq <- function(x, percentage = F){
  if(percentage){
    coef = 100
  }
  else{
    coef = 1
  }
  return(coef * x/sum(x))
}

cumsum_mid <- function(x){
  
  # This function assumes that the 
  # values of x are bounded below by 0.
  
  cumsum_x <- cumsum(x)
  output <- c(x[1] / 2)

  for(i in 1:(length(cumsum_x)-1)){
    midpoint <- (cumsum_x[i+1] + cumsum_x[i])/2
    output <- c(output, midpoint)
  }
  
  return(output)
    
}


w_1 <- function(r){
  return(max(1-2*r, 0))
}

w_2 <- function(r){
  return(1 - abs(2 * r - 1))
}

w_3 <- function(r){
  return(max(2*r - 1, 0))
}

avg_rgb <- function(r){
  return(w_1(r) * red_vec + w_2(r) * yellow_vec + w_3(r) * green_vec)
}

plot_discrete <- function(df, column_name, 
                          title = NULL, colours = NULL, 
                          na_colour = NULL, ...){
  
  disc_df <- get_discrete_df(df, column_name)
  n <- nrow(df)
  
  if(n == 0){
    
    output <- ggplot(disc_df) + 
      geom_bar(aes(x = "Response"), alpha = 0, ...) + 
      labs(title = title, 
           subtitle = "n = 0",
           x = '', 
           y = 'Percentage')
    
  }
  else{
    
    output <- ggplot() + geom_bar(data = disc_df, 
                aes(x = "Response", 
                    y = percentage, 
                    fill = response),
                stat = "identity", ...) + 
      labs(title = title, 
           subtitle = paste0("n = ", n),
           x = '', 
           y = 'Percentage') +
      
      geom_text(data = disc_df, stat = 'identity',
                aes(x = "Response", 
                    y = cum_midpoints,
                    label = paste0(round(percentage),"%")
                ),
                size=4) + 
      
      guides(fill=guide_legend(title="Responses")) + 
      scale_fill_manual(values = colours, na.value = na_colour)
    
  }
  
  return(output)
  
}

get_rel_freq_plot <- function(data_frame, column_name, title = NULL,
  label_x = NULL, label_y = NULL, binwidth = 1, min_val = 1, max_val = 5){

  # Create additional elements
  data = data_frame[, column_name]

  n = length(data)
  avg = mean(data)
  sd = sd(data)
  r = (avg - min_val) / (max_val - min_val)
  
  # Create plot

  plot <- ggplot(data_frame, aes(x = data, y = ..density..)) +
    geom_histogram(aes(fill = ..x..), binwidth = binwidth, 
                   show.legend = F, color = 'black') + 

    scale_fill_gradient2(low = light_grey,
                         mid = do.call("rgb", as.list(avg_rgb(r))),
                         high = light_grey,
                         midpoint = avg) + 
    
    geom_vline(aes(xintercept = avg),
               size = 2, color = 'black', show.legend = F) +
    labs(title = title,
         subtitle = paste0('n = ', n, ", ",
                           'avg = ', round(avg, digits = 1), ", ",
                           'sd = ', round(sd, digits = 1), ", "),
         x = label_x, y = label_y)

  return(plot)
  
}

get_discrete_df <- function(data_frame, col_name, labels_from = NULL, labels_to = NULL){
  
  output <- table(data_frame[col_name], useNA = 'ifany')
  
  if(!is.null(labels_to)){
    
    if(is.null(labels_from)){
      labels_from <- names(output)
    }
    names(output) <- cond_changes(names(output), labels_from, labels_to)
  }
  
  output <- 100 * output / sum(output)
  output <- as.data.frame.table(output)
  output <- cbind(output, rev(cumsum_mid(rev(output[,"Freq"]))))
  colnames(output) <- c("response", "percentage", "cum_midpoints")
  
  return(output)
  
}

para <- function(text, ...){
  # This is intended to overcome a flaw in ggparagraph
  # where 'text' is duplicated and split in between by
  # NA whenever 'text' does not contain a space symbol
  # in the middle.
  output <- ggparagraph(text = paste0(text, " \r"), ...)
  return(output)
}



# Connect to MySQL

mydb = dbConnect(MySQL(), 
   user='root',
   host='localhost',
   password='QuickFingers_99', 
   dbname='glassdoor')

# List all the tables
table_names = dbListTables(mydb)

# Specify company of interest and search for possible
# company IDs.
# n = -1 retrieves all pending items.

query = dbSendQuery(mydb, 
                paste0('select * from overviews where comp_id = ', 
                comp_id, ';'))
comp_overview = fetch(query, n=-1)

# Focus on a single ID (assuming that the result is either unique
# or picked manually).

company_name = comp_overview['company_name'][1,1]

# Assuming that the returned result is unique (or chosen manually),
# query for the company's review data set.

query = dbSendQuery(mydb, 
        paste0('select * from reviews where comp_id = "', comp_id, '";'))
comp_reviews = fetch(query, n=-1)


# ==========
# Numerical dimensions

num_dims_labels <- matrix(
  c('overall_stars', "Overall Stars",
    'work_life_balance', "Work-Life Balance",
    'culture_values', "Cultural Values",
    'career_ops', "Career Opportunities",
    'comp_benefits', "Company Benefits"),
   ncol = 2,
   byrow = T)

num_dims_stat <- matrix(ncol = 3, nrow = 0)
colnames(num_dims_stat) <- c('n', 'avg', 'sd')

num_plots_list <- list()

for(i in 1:nrow(num_dims_labels)){

  column_name = num_dims_labels[i,1]
  title = num_dims_labels[i,2]
  
  # Filter out NA
  num_indices = which(!sapply(comp_reviews[,column_name], is.na))
  dim_frame = comp_reviews[num_indices,]

  # Insert
  num_dims_stat <- rbind(
    num_dims_stat, 
    c(length(dim_frame[,column_name]),
      mean(dim_frame[,column_name]),
      sd(dim_frame[,column_name])
    )
  )

  hist_max = max(table(dim_frame[column_name]))/nrow(dim_frame[column_name])
  
  plot <- get_rel_freq_plot(
    dim_frame,
    column_name = column_name,
    title = title,
    label_x = "Rating",
    label_y = 'Density') + 
    annotate("text", x = 3, y = hist_max / 2 , size = 16, 
             label = round(mean(comp_reviews[,column_name], 
                          na.rm = T), digits = 2)) + 
    theme(panel.background = element_blank(),
          legend.text.align = 0)
  
  num_plots_list[[column_name]] <- plot
  
}

rownames(num_dims_stat) <- num_dims_labels[,1]

# Discrete statistics

recom_plot <- plot_discrete(comp_reviews, 
  column_name = 'recommend', 
  title = "Recommended to Work Here", 
  colours = c(red, green), 
  na_colour = light_grey,
  width = 0.5) + 
  theme(panel.background = element_blank())

outlook_plot <- plot_discrete(comp_reviews, 
  column_name = 'outlook',
  title = "Outlook", 
  colours = c(red, yellow, green), 
  na_colour = light_grey,
  width = 0.5) + 
  theme(panel.background = element_blank())

recom_pos_plot <- plot_discrete(comp_reviews, 
  column_name = 'recom_pos', 
  title = "Approve of CEO", 
  colours = c(green, red, yellow), 
  na_colour = light_grey,
  width = 0.5) + 
  theme(panel.background = element_blank())

# Draw all plots on the same page

rating_plots <- grid.arrange(
  num_plots_list$overall_stars,
  num_plots_list$work_life_balance, 
  num_plots_list$culture_values,
  num_plots_list$career_ops,
  num_plots_list$comp_benefits)

discrete_plots <- grid.arrange(
                 recom_plot,
                 outlook_plot,
                 recom_pos_plot)

# Most helpful review (mhr)
mhr = comp_reviews[which.max(comp_reviews[,'helpful']),][1,]
mhr_disp <- grid.arrange(
  para(text = mhr[1,'review_title'], size = 16),
  para(text = paste0('Reviewer: ', mhr['reviewer'])),
  para(text = paste0('Employment Status: ', mhr['emp_status'])),
  para(text = paste0('Review Date: ', mhr[1,'review_date'])),
  para(text = paste0('Review Helpfulness: ', mhr['helpful'])),
  para(text = '\n'),
  para(text = paste0('Overall Stars: ', mhr['overall_stars'])),
  para(text = paste0('Work-Life Balance: ', mhr['work_life_balance'])),
  para(text = paste0('Cultural Values: ', mhr['culture_values'])),
  para(text = paste0('Career Opportunities: ', mhr['career_ops'])),
  para(text = paste0('Company Benefits: ', mhr['comp_benefits'])),
  para(text = paste0('Recommended: ', mhr['recommend'])),
  para(text = paste0('Approve of CEO: ', mhr['recom_pos'])),
  para(text = paste0('Outlook: ', mhr['outlook'])),
  para(text = '\n'),
  para(text = 'Background: \r', face = 'bold'),
  para(text = mhr['background']),
  para(text = '\n'),
  para(text = 'Pros: \r', face = 'bold'),
  para(text = mhr['pros']),
  para(text = '\n'),
  para(text = 'Cons: \r', face = 'bold'),
  para(text = mhr['cons']),
  para(text = '\n'),
  para(text = 'Advice to management: \r', face = 'bold'),
  para(text = mhr['adv_mngnt']),
  top = textGrob('Most Helpful Review', gp = gpar(
    fontsize = 20, font = 13, col = green)),
  ncol = 1,
  heights = unit(c(16,rep(12, 25)), rep("pt", 26)))


# ==========
# Final plot
grid.arrange(rating_plots, 
             discrete_plots,
             mhr_disp,
             layout_matrix = rbind(c(1, 2, 3)),
             widths = c(1, 1, 1),
             top = textGrob(paste0("Company Overview: ", company_name), 
             gp = gpar(fontsize = 18, font = 2)))


# Disconnect from database
dbDisconnect(mydb)