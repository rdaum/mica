#include <iostream>

#include <log4cpp/OstreamAppender.hh>
#include <log4cpp/BasicLayout.hh>

#include "logging.hh"

using namespace mica;
using namespace std;

static log4cpp::Appender *app;
static log4cpp::Layout *layout;

log4cpp::Category &mica::logger( log4cpp::Category::getInstance("mica_log") );

void mica::initialize_log( bool debug ) {

  app = new log4cpp::OstreamAppender("OStreamAppender", &cerr );
  layout = new log4cpp::BasicLayout( );

  app->setLayout(layout);

  logger.setAdditivity(false);
  
  logger.setAppender(app);

  logger.setPriority( debug ? log4cpp::Priority::DEBUG : log4cpp::Priority::INFO);
}

void mica::close_log() {
  log4cpp::Category::shutdown();
}
