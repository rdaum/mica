/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/

#include <unistd.h>
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <dirent.h>

#ifdef _WIN32
#include <sys/types.h>
#include <sys/timeb.h>
#endif

#include "bin/compile.hh"
#include "persistence/PersistentWorkspace.hh"
#include "types/Atom.hh"
#include "types/Data.hh"
#include "types/Error.hh"
#include "types/Exceptions.hh"
#include "types/GlobalSymbols.hh"
#include "types/List.hh"
#include "types/MetaObjects.hh"
#include "types/Object.hh"
#include "types/Symbol.hh"
#include "types/Symbol.hh"
#include "types/Var.hh"
#include "types/Workspace.hh"
#include "types/Workspaces.hh"
#include "vm/Block.hh"
#include "vm/Message.hh"
#include "vm/OpCodes.hh"
#include "vm/OpCodes.hh"
#include "vm/Scheduler.hh"
#include "vm/Task.hh"

using namespace mica;
using namespace std;

var_vector objects;

Task *compile_task;

class CompileTask : public Task {
 public:
  CompileTask() : Task(0, 0){};

  Var notify(const Var &argument) {
    cerr << "NOTIFY: " << argument << endl;
    return Var();
  }

  void append_child_pointers(child_set &child_list) {
    this->Task::append_child_pointers(child_list);
  }

  void handle_message(const Ref<Message> &reply_message) {
    if (reply_message->isRaise()) {
      mica_string traceback = reply_message->args[1].tostring();
      traceback.push_back('\n');

      cerr << traceback;

    } else if (reply_message->isHalt()) {
      cerr << "Halted." << endl;
    }
  }
};

vector<mica_string> loadDirectory(mica_string path) {
  // look for the path
  struct dirent **namelist;
  int ret = scandir(path.c_str(), &namelist, 0, alphasort);

  if (ret < 0) {
    char errstr[80];
    snprintf(errstr, 80, "unable to load from path (%s)", path.c_str());
    throw internal_error(errstr);
  }

  vector<mica_string> files;
  int count = 0;
  while (count < ret) {
    if (strcmp(namelist[count]->d_name, "..") && strcmp(namelist[count]->d_name, ".") &&
        strcmp(namelist[count]->d_name, "CVS"))
      files.push_back(namelist[count]->d_name);
    count++;
  }
  free(namelist);

  return files;
}

void doDefinition(const Var &obj, const mica_string path) {
  mica_string filename = path;
  filename.append("/DEFINITION");
  std::ifstream file(filename.c_str());

  mica_string source;
  char c;
  while (file.get(c)) {
    source.push_back(c);
  }

  objects.push_back(obj);

  Ref<Block> def_tmp(compile(source));
  obj->declare(Var(VERB_SYM), Symbol::create("DEFINITION_TMP"), Var(def_tmp));

  var_vector args;
  compile_task->send(MetaObjects::SystemMeta, MetaObjects::SystemMeta, obj, obj,
                     Symbol::create("DEFINITION_TMP"),
                     args).perform(compile_task, List::from_vector(args));
}

void loadObject(mica_string path) {
  Var obj = Object::create();

  vector<mica_string> toclear;

  vector<mica_string> dir = loadDirectory(path);

  vector<mica_string>::iterator dirfile = find(dir.begin(), dir.end(), mica_string("DEFINITION"));

  if (dirfile == dir.end())
    throw internal_error("missing DEFINITION file");

  doDefinition(obj, path);

  dir.erase(dirfile);

  for (vector<mica_string>::iterator x = dir.begin(); x != dir.end(); x++) {
    mica_string fname = path;
    fname.push_back('/');
    fname.append(*x);
    std::ifstream file(fname.c_str());
    cerr << fname << endl;
    mica_string source;
    char c;
    while (file.get(c)) {
      source.push_back(c);
    }
    file.close();
    try {
      Var block(compile(source));
      obj->declare(Var(VERB_SYM), Symbol::create(x->c_str()), block);
    } catch (Ref<Error> e) {
      cerr << e << endl;
    }
  }
}

void loadAll(mica_string path) {
  vector<mica_string> dir = loadDirectory(path);

  for (vector<mica_string>::iterator di = dir.begin(); di != dir.end(); di++) {
    mica_string o_path = path;
    o_path.push_back('/');
    o_path.append(*di);
    loadObject(o_path);
  }
}

void loop() {
  Scheduler::instance->start();

  while (Scheduler::instance->run()) {
    usleep(0);
  }

  Scheduler::instance->shutdown();
}

int main(int argc, char *argv[]) {
  Scheduler::initialize();
  initializeOpcodes();

  if (argc != 3) {
    cerr << "Usage: " << argv[0] << " DIRECTORY DATABASE" << endl;
    exit(-1);
  }

  try {
    initSymbols();

    pair<WID, Var> pool_return = Workspace::open(Symbol::create("builtin"));
    Workspaces::instance.setDefault(pool_return.first);

    MetaObjects::initialize(pool_return.second);

    initNatives();

    char *directory = argv[1];
    char *dbname = argv[2];

    pair<WID, Var> p_pool_return(
        PersistentPool::open(Symbol::create(dbname), pool_return.second->asRef<Object>()));

    Workspaces::instance.setDefault(p_pool_return.first);

    compile_task = new CompileTask();
    Scheduler::instance->event_add(compile_task);

    cerr << "Compiling methods" << endl;

    loadAll(directory);

    cerr << "Queueing initialize methods" << endl;

    for (var_vector::iterator x = objects.begin(); x != objects.end(); x++) {
      Var obj = *x;
      var_vector args;

      Var msg = compile_task->send(MetaObjects::SystemMeta, MetaObjects::SystemMeta, obj, obj,
                                   Symbol::create("core_initialize"), args);

      msg.perform(compile_task, List::from_vector(args));
    }

    cerr << "Running VM" << endl;

    loop();

    cerr << "Removing temporary methods" << endl;

    for (var_vector::iterator x = objects.begin(); x != objects.end(); x++) {
      x->remove(Var(VERB_SYM), Symbol::create("DEFINITION_TMP"));
    }

    cerr << "Done" << endl;

    Workspaces::instance.close();

  } catch (Ref<Error> err) {
    cerr << err << endl;
  }

  //  unloadDLLs();
}
