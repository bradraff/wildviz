#' Run the shiny app from the wildviz package
#' @description Runs the wildviz shiny app for data visualization and exploration. Runs on the master, aqi, wildfires, and climate datasets provided within the wildviz package, which are specific to California and whose dates range from 2001 to 2015.
#' @export
#' @importFrom shiny runApp
wildvizApp <- function() {
  shiny::runApp(system.file('shinyApp', package = 'wildviz'))
}
