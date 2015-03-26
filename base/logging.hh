#ifndef MICA_LOGGING_HH
#define MICA_LOGGING_HH

#include <log4cpp/Category.hh>

namespace mica {

extern log4cpp::Category &logger;
extern void initialize_log(bool debug = false);
extern void close_log();

}  // namespace mica

#endif  // MICA_LOGGING_HH
