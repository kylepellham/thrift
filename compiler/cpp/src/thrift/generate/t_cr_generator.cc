/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <sstream>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "thrift/generate/t_oop_generator.h"
#include "thrift/platform.h"
#include "thrift/version.h"

using std::map;
using std::ofstream;
using std::ostringstream;
using std::string;
using std::stringstream;
using std::vector;

static const string endl = "\n"; // avoid ostream << std::endl flushes

/**
 * A subclass of std::ofstream that includes indenting functionality.
 */
class t_cr_ofstream : public std::ofstream {
private:
  int indent_;

public:
  t_cr_ofstream() : std::ofstream(), indent_(0) {}
  explicit t_cr_ofstream(const char* filename,
                         ios_base::openmode mode = ios_base::out,
                         int indent = 0)
    : std::ofstream(filename, mode), indent_(indent) {}

  t_cr_ofstream& indent() {
    for (int i = 0; i < indent_; ++i) {
      *this << "  ";
    }
    return *this;
  }

  void indent_up() { ++indent_; }

  void indent_down() { --indent_; }
};

class t_cr_generator : public t_oop_generator {
public:
  t_cr_generator(t_program* program,
                 const std::map<std::string, std::string>& parsed_options,
                 const std::string&)
    : t_oop_generator(program) {
    namespaced_ = false;
    map<string, string>::const_iterator iter;
    for (iter = parsed_options.begin(); iter != parsed_options.end(); ++iter) {
      if (iter->first.compare("no-skeleton")) {
        continue;
      }
    }

    out_dir_base_ = "gen-cr";
  }

  void init_generator() override;
  void close_generator() override;
  std::string display_name() const;

  void generate_typedef(t_typedef*) override;
  void generate_enum(t_enum* tenum) override;
  void generate_const(t_const* tconst) override;
  void generate_struct(t_struct* tstruct) override;
  void generate_forward_declaration(t_struct* tstruct) override;
  void generate_union(t_struct* tunion);
  void generate_xception(t_struct* txception) override;
  void generate_service(t_service* tservice) override;

  t_cr_ofstream& render_const_value(t_cr_ofstream& out, t_type* type, t_const_value* value);

  /**
   * Struct generation code
   */

  void generate_cr(t_cr_ofstream& out, t_struct* tstruct, bool is_exception);
  void generate_cr_struct(t_cr_ofstream& out, t_struct* tstruct, bool is_exception, bool is_helper);
  void generate_cr_struct_initializer(t_cr_ofstream& out, t_struct* tstruct);
  void generate_cr_struct_required_validator(t_cr_ofstream& out, t_struct* tstruct);
  void generate_cr_union(t_cr_ofstream& out, t_struct* tstruct, bool is_exception);
  void generate_cr_union_validator(t_cr_ofstream& out, t_struct* tstruct);
  void generate_cr_function_helpers(t_function* tfunction);
  void generate_cr_simple_constructor(t_cr_ofstream& out, t_struct* tstruct);
  void generate_cr_simple_exception_constructor(t_cr_ofstream& out, t_struct* tstruct);
  void generate_cr_struct_decleration();
  void generate_serialize_struct(t_cr_ofstream& out, t_struct* tstruct, string prefix);
  void generate_field_constants(t_cr_ofstream& out, t_struct* tstruct);
  void generate_field_constructors(t_cr_ofstream& out, t_struct* tstruct);
  void generate_field_defns(t_cr_ofstream& out, t_struct* tstruct);
  void generate_cr_struct_declaration(t_cr_ofstream& out, t_struct* tstruct, bool is_exception);
  void generate_field_data(t_cr_ofstream& out,
                           t_type* field_type,
                           int key,
                           const std::string& field_name,
                           t_const_value* field_value,
                           bool optional,
                           bool is_union);
  void generate_union_field_data(t_cr_ofstream& out,
                                 t_type* field_type,
                                 const std::string& field_name);

  /**
   * Service-level generation functions
   */

  void generate_service_helpers(t_service* tservice);
  void generate_service_interface(t_service* tservice);
  void generate_service_client(t_service* tservice);
  void generate_service_server(t_service* tservice);
  void generate_process_function(t_service* tservice, t_function* tfunction);
  void generate_service_skeleton(t_service* tservice);

  /**
   * Serialization constructs
   */

  void generate_deserialize_field(t_cr_ofstream& out,
                                  t_field* tfield,
                                  std::string prefix = "",
                                  bool inclass = false);

  void generate_deserialize_struct(t_cr_ofstream& out, t_struct* tstruct, std::string prefix = "");

  void generate_deserialize_container(t_cr_ofstream& out, t_type* ttype, std::string prefix = "");

  void generate_deserialize_set_element(t_cr_ofstream& out, t_set* tset, std::string prefix = "");

  void generate_deserialize_map_element(t_cr_ofstream& out, t_map* tmap, std::string prefix = "");

  void generate_deserialize_list_element(t_cr_ofstream& out,
                                         t_list* tlist,
                                         std::string prefix = "");

  void generate_serialize_field(t_cr_ofstream& out, t_field* tfield, std::string prefix = "");

  void generate_serialize_container(t_cr_ofstream& out, t_type* ttype, std::string prefix = "");

  void generate_serialize_map_element(t_cr_ofstream& out,
                                      t_map* tmap,
                                      std::string kiter,
                                      std::string viter);

  void generate_serialize_set_element(t_cr_ofstream& out, t_set* tmap, std::string iter);

  void generate_serialize_list_element(t_cr_ofstream& out, t_list* tlist, std::string iter);

  void generate_rdoc(t_cr_ofstream& out, t_doc* tdoc);

  /**
   * Helper rendering functions
   */

  t_cr_ofstream& render_crystal_type(t_cr_ofstream& out,
                                     t_type* ttype,
                                     bool optional = false,
                                     bool is_union = false);
  void generate_struct_writer(t_cr_ofstream& out, t_struct* tstruct);
  std::string cr_autogen_comment();
  std::string render_require_thrift();
  std::string render_includes();
  void render_property_type(t_cr_ofstream& out, t_type* field_type,int key);
  std::string declare_field(t_field* tfield);
  std::string type_name(const t_type* ttype);
  std::string full_type_name(const t_type* ttype);
  std::string function_signature(t_function* tfunction, std::string prefix = "");
  std::string argument_list(t_struct* tstruct);
  std::string type_to_enum(t_type* ttype);
  std::string cr_namespace_to_path_prefix(std::string cr_namespace);

  std::vector<std::string> crystal_modules(const t_program* p) {
    std::string ns = p->get_namespace("cr");
    std::vector<std::string> modules;
    if (ns.empty()) {
      return modules;
    }

    std::string::iterator pos = ns.begin();
    while (true) {
      std::string::iterator delim = std::find(pos, ns.end(), '.');
      modules.push_back(capitalize(std::string(pos, delim)));
      pos = delim;
      if (pos == ns.end()) {
        break;
      }
      ++pos;
    }

    return modules;
  }

  void begin_namespace(t_cr_ofstream&, std::vector<std::string>);
  void end_namespace(t_cr_ofstream&, std::vector<std::string>);

private:
  /**
   * File streams
   */

  t_cr_ofstream f_types_;
  t_cr_ofstream f_consts_;
  t_cr_ofstream f_service_;

  std::string namespace_dir_;
  std::string require_prefix_;

  bool namespaced_;
};

/**
 * Prepares for file generation by opening up the necessary file output
 * streams.
 *
 * @param tprogram The program to generate
 */
void t_cr_generator::init_generator() {
  string subdir = get_out_dir();

  // Make output directory
  MKDIR(subdir.c_str());

  if (namespaced_) {
    require_prefix_ = cr_namespace_to_path_prefix(program_->get_namespace("cr"));

    string dir = require_prefix_;
    string::size_type loc;

    while ((loc = dir.find("/")) != string::npos) {
      subdir = subdir + dir.substr(0, loc) + "/";
      MKDIR(subdir.c_str());
      dir = dir.substr(loc + 1);
    }
  }

  namespace_dir_ = subdir;

  // Make output file
  string f_types_name = namespace_dir_ + underscore(program_name_) + "_types.cr";
  f_types_.open(f_types_name.c_str());

  string f_consts_name = namespace_dir_ + underscore(program_name_) + "_constants.cr";
  f_consts_.open(f_consts_name.c_str());

  // Print header
  f_types_ << cr_autogen_comment() << endl << render_require_thrift() << render_includes() << endl;
  begin_namespace(f_types_, crystal_modules(program_));

  f_consts_ << cr_autogen_comment() << endl
            << render_require_thrift() << "require \"" << require_prefix_
            << underscore(program_name_) << "_types\"" << endl
            << endl;
  begin_namespace(f_consts_, crystal_modules(program_));
}

string t_cr_generator::render_includes() {
  const vector<t_program*>& includes = program_->get_includes();
  string result = "";
  for (auto include : includes) {
    t_program* included = include;
    string included_require_prefix = cr_namespace_to_path_prefix(included->get_namespace("cr"));
    string included_name = included->get_name();
    result
        += "require \"./"/* + included_require_prefix */ + underscore(included_name) + "_types.cr\"" + endl;
  }
  if (includes.size() > 0) {
    result += endl;
  }
  return result;
}

/**
 * Autogen'd comment
 */
string t_cr_generator::cr_autogen_comment() {
  return std::string("#\n") + "# Autogenerated by Thrift Compiler (" + THRIFT_VERSION + ")\n"
         + "#\n" + "# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING\n" + "#\n";
}

/**
 * Closes the type files
 */
void t_cr_generator::close_generator() {
  // Close types file
  end_namespace(f_types_, crystal_modules(program_));
  end_namespace(f_consts_, crystal_modules(program_));
  f_types_.close();
  f_consts_.close();
}

/**
 * Generates a typedef. This is not done in Crystal, types are all implicit.
 *
 * @param ttypedef The type definition
 */
void t_cr_generator::generate_typedef(t_typedef*) {}

/**
 * Generates code for an enumerated type. Done using a class to scope
 * the values.
 *
 * @param tenum The enumeration
 */
void t_cr_generator::generate_enum(t_enum* tenum) {
  f_types_.indent() << "enum " << capitalize(tenum->get_name()) << endl;
  f_types_.indent_up();

  vector<t_enum_value*> constants = tenum->get_constants();
  vector<t_enum_value*>::iterator c_iter;
  for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter) {
    int value = (*c_iter)->get_value();

    string name = capitalize(lowercase((*c_iter)->get_name()));

    generate_rdoc(f_types_, *c_iter);
    f_types_.indent() << name << " = " << value << endl;
  }
  f_types_.indent_down();
  f_types_.indent() << "end" << endl << endl;
}

/**
 * Generate a constant value
 */
void t_cr_generator::generate_const(t_const* tconst) {
  t_type* type = tconst->get_type();
  string name = tconst->get_name();
  t_const_value* value = tconst->get_value();

  name = uppercase(name);

  f_consts_.indent() << name << " = ";
  render_const_value(f_consts_, type, value) << endl << endl;
}

/**
 * Prints the value of a constant with the given type. Note that type checking
 * is NOT performed in this function as it is always run beforehand using the
 * validate_types method in main.cc
 */
t_cr_ofstream& t_cr_generator::render_const_value(t_cr_ofstream& out,
                                                  t_type* type,
                                                  t_const_value* value) {
  type = get_true_type(type);
  if (type->is_base_type()) {
    t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
    switch (tbase) {
    case t_base_type::TYPE_STRING:
      out << "%q(" << get_escaped_string(value) << ')';
      break;
    case t_base_type::TYPE_BOOL:
      out << (value->get_integer() > 0 ? "true" : "false");
      break;
    case t_base_type::TYPE_I8:
    case t_base_type::TYPE_I16:
    case t_base_type::TYPE_I32:
    case t_base_type::TYPE_I64:
      out << value->get_integer();
      break;
    case t_base_type::TYPE_DOUBLE:
      if (value->get_type() == t_const_value::CV_INTEGER) {
        out << value->get_integer();
      } else {
        out << value->get_double();
      }
      break;
    default:
      throw "compiler error: no const of base type " + t_base_type::t_base_name(tbase);
    }
  } else if (type->is_enum()) {
    out.indent() << value->get_integer();
  } else if (type->is_struct() || type->is_xception()) {
    out << full_type_name(type) << ".new(" << endl;
    out.indent_up();
    const vector<t_field*>& fields = ((t_struct*)type)->get_members();
    vector<t_field*>::const_iterator f_iter;
    const map<t_const_value*, t_const_value*, t_const_value::value_compare>& val = value->get_map();
    map<t_const_value*, t_const_value*, t_const_value::value_compare>::const_iterator v_iter;
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      t_type* field_type = nullptr;
      for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
        if ((*f_iter)->get_name() == v_iter->first->get_string()) {
          field_type = (*f_iter)->get_type();
        }
      }
      if (field_type == nullptr) {
        throw "type error: " + type->get_name() + " has no field " + v_iter->first->get_string();
      }
      out.indent();
      render_const_value(out, g_type_string, v_iter->first) << " => ";
      render_const_value(out, field_type, v_iter->second) << "," << endl;
    }
    out.indent_down();
    out.indent() << ")";
  } else if (type->is_map()) {
    t_type* ktype = ((t_map*)type)->get_key_type();
    t_type* vtype = ((t_map*)type)->get_val_type();
    render_crystal_type(out, type);

    out << "{" << endl;
    out.indent_up();
    const map<t_const_value*, t_const_value*, t_const_value::value_compare>& val = value->get_map();
    map<t_const_value*, t_const_value*, t_const_value::value_compare>::const_iterator v_iter;
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      out.indent();
      render_const_value(out, ktype, v_iter->first) << " => ";
      render_const_value(out, vtype, v_iter->second) << "," << endl;
    }
    out.indent_down();
    out.indent() << "}";
  } else if (type->is_list() || type->is_set()) {
    t_type* etype;
    if (type->is_list()) {
      etype = ((t_list*)type)->get_elem_type();
    } else {
      etype = ((t_set*)type)->get_elem_type();
    }
    render_crystal_type(out, type);
    if (value->get_list().empty()) {
      out << ".new";
    } else {
      out << "{" << endl;
      out.indent_up();
      const vector<t_const_value*>& val = value->get_list();
      vector<t_const_value*>::const_iterator v_iter;
      for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
        out.indent();
        render_const_value(out, etype, *v_iter) << "," << endl;
      }
      out.indent_down();
      out.indent() << "}";
    }
  } else {
    throw "CANNOT GENERATE CONSTANT FOR TYPE: " + type->get_name();
  }
  return out;
}

/**
 * Generates a Crystal struct
 */
void t_cr_generator::generate_struct(t_struct* tstruct) {
  if (tstruct->is_union()) {
    generate_cr_union(f_types_, tstruct, false);
  } else {
    generate_cr_struct(f_types_, tstruct, false, false);
  }
}

/**
 * Generates the "forward declarations" for Crystal structs.
 * These are simply a declaration of each class with proper inheritance.
 * The rest of the struct is still generated in generate_struct as has
 * always been the case. These declarations allow thrift to generate valid
 * Crystal in cases where thrift structs rely on recursive definitions.
 */
void t_cr_generator::generate_forward_declaration(t_struct* tstruct) {
  generate_cr_struct_declaration(f_types_, tstruct, tstruct->is_xception());
}

void t_cr_generator::generate_cr_struct_declaration(t_cr_ofstream& out,
                                                    t_struct* tstruct,
                                                    bool is_exception) {
  out.indent() << "class " << type_name(tstruct);
  if (is_exception) {
    out << " < ::Thrift::Exception";
  }
  out << "; end" << endl << endl;
}

/**
 * Generates a struct definition for a thrift exception. Basically the same
 * as a struct but extends the Exception class.
 *
 * @param txception The struct definition
 */
void t_cr_generator::generate_xception(t_struct* txception) {
  generate_cr_struct(f_types_, txception, true, false);
}

/**
 * Generates a crystal struct
 */
void t_cr_generator::generate_cr_struct(t_cr_ofstream& out,
                                        t_struct* tstruct,
                                        bool is_exception = false,
                                        bool is_helper = false) {
  generate_rdoc(out, tstruct);
  out.indent() << "class " << type_name(tstruct);
  if (is_exception) {
    out << " < Exception";
  }
  out << endl;
  out.indent_up();
  out.indent() << "include ::Thrift::Struct" << endl << endl;

  if (is_exception) {
    generate_cr_simple_exception_constructor(out, tstruct);
  }

  // generate_field_constants(out, tstruct);
  generate_field_defns(out, tstruct);
  generate_field_constructors(out, tstruct);
  generate_cr_struct_required_validator(out, tstruct);
  // if (is_helper) {
    // generate_struct_writer(out, tstruct);
 // }

  out.indent_down();
  out.indent() << "end" << endl << endl;
}

void generate_cr_struct_initializer(t_cr_ofstream& out, t_struct* tstruct)
{
  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter = fields.cbegin();
  out.indent_up();
  out.indent() << "def initialize(";
}

/**
 * Generates a ruby union
 */
void t_cr_generator::generate_cr_union(t_cr_ofstream& out,
                                       t_struct* tstruct,
                                       bool is_exception = false) {
  (void)is_exception;
  generate_rdoc(out, tstruct);
  out.indent() << "class " << type_name(tstruct) << endl;
  out.indent_up();
  out.indent() << "include ::Thrift::Union" << endl;

  // generate_field_constructors(out, tstruct);

  generate_field_constants(out, tstruct);
  generate_field_defns(out, tstruct);
  generate_cr_union_validator(out, tstruct);

  out.indent_down();
  out.indent() << "end" << endl << endl;
}

void t_cr_generator::generate_field_constructors(t_cr_ofstream& out, t_struct* tstruct) {

  // out.indent() << "class << self" << endl;
  // out.indent_up();

  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;


  out.indent() << "def initialize(";
  bool first = true;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    // if (f_iter != fields.begin()) {
    //   out << endl;
    // }
    std::string field_name = "@" + (*f_iter)->get_name();
    if (first) {
      first = false;
    } else {
      out << ", ";
    }
    out << field_name;
    // out.indent() << "  " << tstruct->get_name() << ".new(:" << field_name << ", val)" << endl;
    // out.indent() << "end" << endl;
  }
  out << ")" << endl;
  out.indent() << "end" << endl << endl;
  if (!first) {
    out.indent() << "def initialize; end" << endl;
  }
  // out.indent_down();
  // out.indent() << "end" << endl;

  out << endl;
}

void t_cr_generator::generate_cr_simple_exception_constructor(t_cr_ofstream& out,
                                                              t_struct* tstruct) {
  const vector<t_field*>& members = tstruct->get_members();

  if (members.size() == 1) {
    vector<t_field*>::const_iterator m_iter = members.begin();

    if ((*m_iter)->get_type()->is_string()) {
      string name = (*m_iter)->get_name();

      out.indent() << "def initialize(message=nil)" << endl;
      out.indent_up();
      out.indent() << "super()" << endl;
      out.indent() << "self." << name << " = message" << endl;
      out.indent_down();
      out.indent() << "end" << endl << endl;

      if (name != "message") {
        out.indent() << "def message; " << name << " end" << endl << endl;
      }
    }
  }
}

void t_cr_generator::generate_field_constants(t_cr_ofstream& out, t_struct* tstruct) {
  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;

  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    std::string field_name = (*f_iter)->get_name();
    std::string cap_field_name = upcase_string(field_name);

    out.indent() << cap_field_name << " = " << (*f_iter)->get_key() << endl;
  }
  out << endl;
}

t_cr_ofstream& t_cr_generator::render_crystal_type(t_cr_ofstream& out,
                                                   t_type* ttype,
                                                   bool optional,
                                                   bool is_union) {

  ttype = get_true_type(ttype);
  if (ttype->is_base_type()) {
    t_base_type::t_base base = ((t_base_type*)ttype)->get_base();
    switch (base) {
    case t_base_type::TYPE_BOOL:
      out << "Bool";
      break;
    case t_base_type::TYPE_STRING:
      out << "String";
      break;
    case t_base_type::TYPE_VOID:
      out << "Nil";
      break;
    case t_base_type::TYPE_UUID:
    case t_base_type::TYPE_I8:
      out << "Int8";
      break;
    case t_base_type::TYPE_I16:
      out << "Int16";
      break;
    case t_base_type::TYPE_I32:
      out << "Int32";
      break;
    case t_base_type::TYPE_I64:
      out << "Int64";
      break;
    case t_base_type::TYPE_DOUBLE:
      out << "Float64";
      break;
    default:
      break;
    }
  } else if (ttype->is_enum()) {
    out << full_type_name(ttype);
  } else if (ttype->is_xception() || ttype->is_struct()) {
    out << full_type_name(ttype);
  } else if (ttype->is_map()) {
    out << "Hash(";
    render_crystal_type(out, ((t_map*)ttype)->get_key_type()) << ", ";
    render_crystal_type(out, ((t_map*)ttype)->get_val_type()) << ")";
  } else if (ttype->is_list() || ttype->is_set()) {
    t_type* ele_type;
    if (ttype->is_list()) {
      ele_type = ((t_list*)ttype)->get_elem_type();
      out << "Array(";
      render_crystal_type(out, ele_type) << ')';
    } else {
      ele_type = ((t_set*)ttype)->get_elem_type();
      out << "Set(";
      render_crystal_type(out, ele_type) << ')';
    }
  }
  if (optional && !is_union) {
    out << " | Nil";
  }
  return out;
}

void t_cr_generator::generate_field_defns(t_cr_ofstream& out, t_struct* tstruct) {
  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;

  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    out.indent() << "@[Property(";
    render_property_type(out, (*f_iter)->get_type(), (*f_iter)->get_key());
    out << ")]" << endl;
    if (tstruct->is_union()) {
      out.indent() << "union_property ";
      generate_union_field_data(out, (*f_iter)->get_type(), (*f_iter)->get_name());
    } else {
      out.indent() << "property ";
      generate_field_data(out, (*f_iter)->get_type(), (*f_iter)->get_key(), (*f_iter)->get_name(), (*f_iter)->get_value(),
                          (*f_iter)->get_req() == t_field::T_OPTIONAL, tstruct->is_union());
    }
    out << endl;
  }
  out << endl;
}

void t_cr_generator::generate_field_data(t_cr_ofstream& out,
                                         t_type* field_type,
                                         int key,
                                         const std::string& field_name = "",
                                         t_const_value* field_value = nullptr,
                                         bool optional = false,
                                         bool is_union = false) {
  // field_type = get_true_type(field_type);
  out << field_name << " : ";
  render_crystal_type(out, get_true_type(field_type), true);
  if (field_value != nullptr) {
    out << " = ";
    render_const_value(out, field_type, field_value);
  }
}

void t_cr_generator::generate_union_field_data(t_cr_ofstream& out,
                                               t_type* field_type,
                                               const std::string& field_name) {

  out << field_name << " : ";
  render_crystal_type(out, get_true_type(field_type), true);
}

void t_cr_generator::begin_namespace(t_cr_ofstream& out, vector<std::string> modules) {
  for (auto& module : modules) {
    out.indent() << "module " << module << endl;
    out.indent_up();
  }
}

void t_cr_generator::end_namespace(t_cr_ofstream& out, vector<std::string> modules) {
  for (vector<std::string>::reverse_iterator m_iter = modules.rbegin(); m_iter != modules.rend();
       ++m_iter) {
    out.indent_down();
    out.indent() << "end" << endl;
  }
}

/**
 * Generates a thrift service.
 *
 * @param tservice The service definition
 */
void t_cr_generator::generate_service(t_service* tservice) {
  string f_service_name = namespace_dir_ + underscore(service_name_) + ".cr";
  f_service_.open(f_service_name.c_str());

  f_service_ << cr_autogen_comment() << endl << render_require_thrift();

  if (tservice->get_extends() != nullptr) {
    if (namespaced_) {
      f_service_ << "require \"./"
                 << cr_namespace_to_path_prefix(
                        tservice->get_extends()->get_program()->get_namespace("cr"))
                 << underscore(tservice->get_extends()->get_name()) << ".cr\"" << endl;
    } else {
      f_service_ << "require \"./" << require_prefix_
                 << underscore(tservice->get_extends()->get_name()) << ".cr\"" << endl;
    }
  }

  f_service_ << "require \"./" << require_prefix_ << underscore(program_name_) << "_types.cr\"" << endl
             << endl;

  begin_namespace(f_service_, crystal_modules(tservice->get_program()));

  f_service_.indent() << "module " << capitalize(tservice->get_name()) << endl;
  f_service_.indent_up();

  // Generate the three main parts of the service (well, two for now in PHP)
  generate_service_client(tservice);
  generate_service_server(tservice);
  generate_service_helpers(tservice);
  generate_service_skeleton(tservice);

  f_service_.indent_down();
  f_service_.indent() << "end" << endl << endl;

  end_namespace(f_service_, crystal_modules(tservice->get_program()));

  // Close service file
  f_service_.close();
}

/**
 * Generates helper functions for a service.
 *
 * @param tservice The service to generate a header definition for
 */
void t_cr_generator::generate_service_helpers(t_service* tservice) {
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::iterator f_iter;

  f_service_.indent() << "# HELPER FUNCTIONS AND STRUCTURES" << endl << endl;

  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    t_struct* ts = (*f_iter)->get_arglist();
    generate_cr_struct(f_service_, ts, false, true);
    generate_cr_function_helpers(*f_iter);
  }
}

/**
 * Generates a struct and helpers for a function.
 *
 * @param tfunction The function
 */
void t_cr_generator::generate_cr_function_helpers(t_function* tfunction) {
  t_struct result(program_, tfunction->get_name() + "_result");
  t_field success(tfunction->get_returntype(), "success", 0);
  if (!tfunction->get_returntype()->is_void()) {
    result.append(&success);
  }

  t_struct* xs = tfunction->get_xceptions();
  const vector<t_field*>& fields = xs->get_members();
  vector<t_field*>::const_iterator f_iter;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    result.append(*f_iter);
  }
  generate_cr_struct(f_service_, &result, false, true);
}

/**
 * Generates a service client definition.
 *
 * @param tservice The service to generate a server for.
 */
void t_cr_generator::generate_service_client(t_service* tservice) {
  string extends = "";
  string extends_client = "";
  if (tservice->get_extends() != nullptr) {
    extends = full_type_name(tservice->get_extends());
    extends_client = " < " + extends + "::Client ";
  }

  f_service_.indent() << "class Client" << extends_client << endl;
  f_service_.indent_up();

  f_service_.indent() << "include ::Thrift::Client" << endl << endl;

  // Generate client method implementations
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::const_iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    t_struct* arg_struct = (*f_iter)->get_arglist();
    const vector<t_field*>& fields = arg_struct->get_members();
    vector<t_field*>::const_iterator fld_iter;
    string funname = (*f_iter)->get_name();

    // Open function
    f_service_.indent() << "def " << function_signature(*f_iter) << endl;
    f_service_.indent_up();
    f_service_.indent() << "send_" << funname << "(";

    bool first = true;
    for (fld_iter = fields.begin(); fld_iter != fields.end(); ++fld_iter) {
      if (first) {
        first = false;
      } else {
        f_service_ << ", ";
      }
      f_service_ << (*fld_iter)->get_name();
    }
    f_service_ << ")" << endl;

    if (!(*f_iter)->is_oneway()) {
      f_service_.indent();
      if (!(*f_iter)->get_returntype()->is_void()) {
        f_service_ << "return ";
      }
      f_service_ << "recv_" << funname << "()" << endl;
    }
    f_service_.indent_down();
    f_service_.indent() << "end" << endl;
    f_service_ << endl;

    f_service_.indent() << "def send_" << function_signature(*f_iter) << endl;
    f_service_.indent_up();

    std::string argsname = capitalize((*f_iter)->get_name() + "_args");
    std::string messageSendProc = (*f_iter)->is_oneway() ? "send_oneway_message" : "send_message";

    f_service_.indent() << messageSendProc << "(\"" << funname << "\", " << argsname;

    for (fld_iter = fields.begin(); fld_iter != fields.end(); ++fld_iter) {
      f_service_ << ", " << (*fld_iter)->get_name() << ": " << (*fld_iter)->get_name();
    }

    f_service_ << ")" << endl;

    f_service_.indent_down();
    f_service_.indent() << "end" << endl;

    if (!(*f_iter)->is_oneway()) {
      std::string resultname = capitalize((*f_iter)->get_name() + "_result");
      t_struct noargs(program_);

      t_function recv_function((*f_iter)->get_returntype(), string("recv_") + (*f_iter)->get_name(),
                               &noargs);
      // Open function
      f_service_ << endl;
      f_service_.indent() << "def " << function_signature(&recv_function) << endl;
      f_service_.indent_up();

      f_service_.indent() << "fname, mtype, rseqid = receive_message_begin()" << endl;
      f_service_.indent() << "handle_exception(mtype)" << endl;

      f_service_.indent() << "if reply_seqid(rseqid)==false" << endl;
      f_service_.indent() << "  raise \"seqid reply faild\"" << endl;
      f_service_.indent() << "end" << endl;

      f_service_.indent() << "result = receive_message(" << resultname << ")" << endl;

      // Careful, only return _result if not a void function
      if (!(*f_iter)->get_returntype()->is_void()) {
        f_service_.indent() << "return result.success unless result.success.nil?" << endl;
      }

      t_struct* xs = (*f_iter)->get_xceptions();
      const std::vector<t_field*>& xceptions = xs->get_members();
      vector<t_field*>::const_iterator x_iter;
      for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
        f_service_.indent() << "raise result." << (*x_iter)->get_name() << " unless result."
                            << (*x_iter)->get_name() << ".nil?" << endl;
      }

      // Careful, only return _result if not a void function
      if ((*f_iter)->get_returntype()->is_void()) {
        f_service_.indent() << "return" << endl;
      } else {
        f_service_.indent() << "raise "
                               "::Thrift::ApplicationException.new(::Thrift::ApplicationException::"
                               "MISSING_RESULT, \""
                            << (*f_iter)->get_name() << " failed: unknown result\")" << endl;
      }

      // Close function
      f_service_.indent_down();
      f_service_.indent() << "end" << endl << endl;
    }
  }

  f_service_.indent_down();
  f_service_.indent() << "end" << endl << endl;
}

/**
 * Generates a service server definition.
 *
 * @param tservice The service to generate a server for.
 */
void t_cr_generator::generate_service_server(t_service* tservice) {
  // Generate the dispatch methods
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::iterator f_iter;
  std::string svcname = tservice->get_name();

  string extends = "";
  string extends_processor = "";
  if (tservice->get_extends()) {
    extends = full_type_name(tservice->get_extends());
    extends_processor = " < " + extends + "::Processor ";
  }

  // Generate the header portion
  f_service_.indent() << "class Processor(T)" << endl;
  f_service_.indent_up();
  f_service_.indent() << "include ::Thrift::Processor" << extends_processor << endl;
  f_service_.indent() << "@handler : T" << endl << endl;

  // f_service_.indent() << "include ::Thrift::Processor" << endl << endl;

  // Generate the process subfunctions
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    generate_process_function(tservice, *f_iter);
  }

  f_service_.indent_down();
  f_service_.indent() << "end" << endl << endl;
}

/**
 * Generates a process function definition.
 *
 * @param tfunction The function to write a dispatcher for
 */
void t_cr_generator::generate_process_function(t_service* tservice, t_function* tfunction) {
  (void)tservice;
  // Open function
  f_service_.indent() << "def process_" << decapitalize(tfunction->get_name()) << "(seqid, iprot, oprot)" << endl;
  f_service_.indent_up();

  string argsname = capitalize(tfunction->get_name()) + "_args";
  string resultname = capitalize(tfunction->get_name()) + "_result";

  f_service_.indent() << "args = read_args(iprot, " << argsname << ")" << endl;

  t_struct* xs = tfunction->get_xceptions();
  const std::vector<t_field*>& xceptions = xs->get_members();
  vector<t_field*>::const_iterator x_iter;

  // Declare result for non oneway function
  if (!tfunction->is_oneway()) {
    f_service_.indent() << "result = " << resultname << ".new" << endl;
  }

  // Try block for a function with exceptions
  if (xceptions.size() > 0) {
    f_service_.indent() << "begin" << endl;
    f_service_.indent_up();
  }

  // Generate the function call
  t_struct* arg_struct = tfunction->get_arglist();
  const std::vector<t_field*>& fields = arg_struct->get_members();
  vector<t_field*>::const_iterator f_iter;

  f_service_.indent();
  if (!tfunction->is_oneway() && !tfunction->get_returntype()->is_void()) {
    f_service_ << "result.success = ";
  }
  f_service_ << "@handler." << decapitalize(tfunction->get_name()) << "(";
  bool first = true;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    if (first) {
      first = false;
    } else {
      f_service_ << ", ";
    }
    f_service_ << "args." << (*f_iter)->get_name();
  }
  f_service_ << ")" << endl;

  if (!tfunction->is_oneway() && xceptions.size() > 0) {
    f_service_.indent_down();
    for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
      f_service_.indent() << "rescue " << (*x_iter)->get_name() << " : "
                          << full_type_name((*x_iter)->get_type()) << endl;
      if (!tfunction->is_oneway()) {
        f_service_.indent_up();
        f_service_.indent() << "result." << (*x_iter)->get_name() << " = " << (*x_iter)->get_name()
                            << endl;
        f_service_.indent_down();
      }
    }
    f_service_.indent() << "end" << endl;
  }

  // Shortcut out here for oneway functions
  if (tfunction->is_oneway()) {
    f_service_.indent() << "return" << endl;
    f_service_.indent_down();
    f_service_.indent() << "end" << endl << endl;
    return;
  }

  f_service_.indent() << "write_result(result, oprot, \"" << tfunction->get_name() << "\", seqid)"
                      << endl;

  // Close function
  f_service_.indent_down();
  f_service_.indent() << "end" << endl << endl;
}

/**
 * Renders a function signature of the form 'type name(args)'
 *
 * @param tfunction Function definition
 * @return String of rendered function definition
 */
string t_cr_generator::function_signature(t_function* tfunction, string prefix) {
  // TODO(mcslee): Nitpicky, no ',' if argument_list is empty
  return prefix + decapitalize(tfunction->get_name()) + "(" + argument_list(tfunction->get_arglist()) + ")";
}

/**
 * Renders the require of thrift itself, and possibly of the rubygems dependency.
 */
string t_cr_generator::render_require_thrift() {
  return "require \"thrift\"\n";
}

/**
 * Renders a field list
 */
string t_cr_generator::argument_list(t_struct* tstruct) {
  string result = "";

  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;
  bool first = true;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    if (first) {
      first = false;
    } else {
      result += ", ";
    }
    result += (*f_iter)->get_name();
  }
  return result;
}

string t_cr_generator::type_name(const t_type* ttype) {
  string prefix = "";

  string name = ttype->get_name();
  if (ttype->is_struct() || ttype->is_xception() || ttype->is_enum()) {
    name = capitalize(ttype->get_name());
  }

  return prefix + name;
}

string t_cr_generator::full_type_name(const t_type* ttype) {
  string prefix = "::";
  vector<std::string> modules = crystal_modules(ttype->get_program());
  for (auto& module : modules) {
    prefix += module + "::";
  }
  return prefix + type_name(ttype);
}

/**
 * Converts the parse type to a Ruby tyoe
 */
string t_cr_generator::type_to_enum(t_type* type) {
  type = get_true_type(type);

  if (type->is_base_type()) {
    t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
    switch (tbase) {
    case t_base_type::TYPE_VOID:
      throw "NO T_VOID CONSTRUCT";
    case t_base_type::TYPE_STRING:
      return "::Thrift::Types::STRING";
    case t_base_type::TYPE_BOOL:
      return "::Thrift::Types::BOOL";
    case t_base_type::TYPE_I8:
      return "::Thrift::Types::BYTE";
    case t_base_type::TYPE_I16:
      return "::Thrift::Types::I16";
    case t_base_type::TYPE_I32:
      return "::Thrift::Types::I32";
    case t_base_type::TYPE_I64:
      return "::Thrift::Types::I64";
    case t_base_type::TYPE_DOUBLE:
      return "::Thrift::Types::DOUBLE";
    default:
      throw "compiler error: unhandled type";
    }
  } else if (type->is_enum()) {
    return "::Thrift::Types::I32";
  } else if (type->is_struct() || type->is_xception()) {
    return "::Thrift::Types::STRUCT";
  } else if (type->is_map()) {
    return "::Thrift::Types::MAP";
  } else if (type->is_set()) {
    return "::Thrift::Types::SET";
  } else if (type->is_list()) {
    return "::Thrift::Types::LIST";
  }

  throw "INVALID TYPE IN type_to_enum: " + type->get_name();
}

string t_cr_generator::cr_namespace_to_path_prefix(string cr_namespace) {
  string namespaces_left = cr_namespace;
  string::size_type loc;

  string path_prefix = "";

  while ((loc = namespaces_left.find(".")) != string::npos) {
    path_prefix = path_prefix + underscore(namespaces_left.substr(0, loc)) + "/";
    namespaces_left = namespaces_left.substr(loc + 1);
  }
  if (namespaces_left.size() > 0) {
    path_prefix = path_prefix + underscore(namespaces_left) + "/";
  }
  return path_prefix;
}

void t_cr_generator::generate_rdoc(t_cr_ofstream& out, t_doc* tdoc) {
  if (tdoc->has_doc()) {
    out.indent();
    generate_docstring_comment(out, "", "# ", tdoc->get_doc(), "");
  }
}

void t_cr_generator::generate_cr_struct_required_validator(t_cr_ofstream& out, t_struct* tstruct) {
  out.indent() << "def validate" << endl;
  out.indent_up();

  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;

  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    t_field* field = (*f_iter);
    if (field->get_req() == t_field::T_REQUIRED) {
      out.indent() << "raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, "
                      "\"Required field "
                   << field->get_name() << " is unset!\")";
      if (field->get_type()->is_bool()) {
        out << " if @" << field->get_name() << ".nil?";
      } else {
        out << " unless @" << field->get_name();
      }
      out << endl;
    }
  }

  // if field is an enum, check that its value is valid
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    t_field* field = (*f_iter);

    if (field->get_type()->is_enum()) {
      out.indent() << "unless @" << field->get_name() << ".nil?" << endl;
      out.indent_up();
      out.indent() << "raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, "
                      "\"Invalid value of field "
                   << field->get_name() << "!\")" << endl;
      out.indent_down();
      out.indent() << "end" << endl;
    }
  }

  out.indent_down();
  out.indent() << "end" << endl << endl;
}

void t_cr_generator::generate_deserialize_field(t_cr_ofstream& out,
                                                t_field* tfield,
                                                std::string prefix,
                                                bool inclass)
{

}

void t_cr_generator::generate_cr_union_validator(t_cr_ofstream& out, t_struct* tstruct) {
  out.indent() << "def validate" << endl;
  out.indent_up();

  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;

  out.indent() << "raise(StandardError, \"Union fields are not set.\") if get_set_field.nil? || "
                  "get_value.nil?"
               << endl;

  // if field is an enum, check that its value is valid
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    const t_field* field = (*f_iter);

    if (field->get_type()->is_enum()) {
      out.indent() << "if get_set_field == :" << field->get_name() << endl;
      out.indent() << "  raise "
                      "::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, "
                      "\"Invalid value of field "
                   << field->get_name() << "!\") unless " << full_type_name(field->get_type())
                   << "::VALID_VALUES.include?(get_value)" << endl;
      out.indent() << "end" << endl;
    }
  }

  out.indent_down();
  out.indent() << "end" << endl << endl;
}

std::string t_cr_generator::display_name() const {
  return "Crystal";
}

/**
 * Generates a skeleton file of a server
 *
 * @param tservice The service to generate a server for.
 */
void t_cr_generator::generate_service_skeleton(t_service* tservice) {
  string svcname = tservice->get_name();

  // Service implementation file includes
  string f_skeleton_name = get_out_dir() + lowercase(svcname) + "_server.skeleton.cr";

  string ns = ""; //namespace_prefix(tservice->get_program()->get_namespace("cr"));

  ofstream_with_content_based_conditional_update f_skeleton;
  f_skeleton.open(f_skeleton_name.c_str());
  f_skeleton << "# This autogenerated skeleton file illustrates how to build a server." << endl
             << "# You should copy it to another filename to avoid overwriting it." << endl << endl
            //  << "require \"" << get_include_prefix(*get_program()) << svcname << ".h\"" << endl
             << "require \"thrift\"" << endl
             << "include Thrift" << endl;

  // the following code would not compile:
  // using namespace ;
  // using namespace ::;
  if ((!ns.empty()) && (ns.compare(" ::") != 0)) {
    f_skeleton << "using " << string(ns, 0, ns.size() - 2) << endl << endl;
  }

  f_skeleton << "class " << svcname << "Handler" << endl;
  indent_up();
  f_skeleton << indent() << "def initialize" << endl
             << "  # Your initialization goes here" << endl << indent() << "end" << endl << endl;

  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    // generate_java_doc(f_skeleton, *f_iter);
    f_skeleton << indent() << "def " << function_signature(*f_iter, "") << endl << indent()
               << "  # Your implementation goes here" << endl << indent() << "  puts(\""
               << (*f_iter)->get_name() << "\\n\");" << endl << indent() << "end" << endl << endl;
  }

  indent_down();
  f_skeleton << "end" << endl << endl;

  f_skeleton << indent() << "def some_skeleton_main" << endl;
  indent_up();
  f_skeleton
      << indent() << "port = 9090" << endl << indent()
      << "handler = " << svcname << "Handler.new" << endl << indent()
      << "processor = " << svcname << "::Processor.new(handler)" << endl
      << indent() << "serverTransport = ServerSocket.new(port)"
      << endl << indent()
      << "transportFactory = BufferedTransportFactory.new" << endl
      << indent() << "protocolFactory = BinaryProtocolFactory.new"
      << endl << endl << indent()
      << "server = SimpleServer.new(processor, serverTransport, transportFactory, protocolFactory);"
      << endl << indent() << "server.serve" << endl << indent() << "return 0" << endl;
  indent_down();
  f_skeleton << "end" << endl << endl;

  // Close the files
  f_skeleton.close();
}

void t_cr_generator::generate_struct_writer(t_cr_ofstream& out, t_struct* tstruct)
{
  string name = tstruct->get_name();
  const vector<t_field*>& fields = tstruct->get_sorted_members();
  vector<t_field*>::const_iterator f_iter;


  out.indent() << "def write(oprot : Thrift::BaseProtocol)" << endl;
  out.indent_up();
  out.indent() << "oprot.write_struct_begin(" << capitalize(name) << ")" << endl;
  std::for_each(std::begin(fields), std::end(fields), [&](auto field) {
    out.indent() << "oprot.write_field_begin(\"" << field->get_name() << "\", "
                 << type_to_enum(field->get_type()) << ", "
                 << field->get_key() << ")" << endl;
    generate_serialize_field(out, field);
    out.indent() << "oprot.write_field_end" << endl;
  });
  out.indent() << "oprot.write_struct_end" << endl;
  out.indent_down();
  out.indent() << "end" << endl;
  
}

void t_cr_generator::generate_serialize_field(t_cr_ofstream& out, t_field* tfield, std::string)
{
  t_type* type = get_true_type(tfield->get_type());

  string name = tfield->get_name();

  if (type->is_struct() || type->is_xception()) {
    generate_serialize_struct(out, (t_struct*)type, name);
  } else if (type->is_container()) {

  } else if (type->is_base_type() || type->is_enum()) {
    out.indent() << "oprot.";
    if (type->is_base_type()) {
      t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
      switch(tbase) {
      case t_base_type::TYPE_STRING:
        if (type->is_binary()) {
          out << "write_binary(" << name << ");";
        } else {
          out << "write_string(" << name << ");";
        }
        break;
      case t_base_type::TYPE_BOOL:
        out << "write_bool(" << name << ");";
        break;
      case t_base_type::TYPE_I8:
        out << "write_byte(" << name << ");";
        break;
      case t_base_type::TYPE_I16:
        out << "write_i16(" << name << ");";
        break;
      case t_base_type::TYPE_I32:
        out << "write_i32(" << name << ");";
        break;
      case t_base_type::TYPE_I64:
        out << "write_i64(" << name << ");";
        break;
      case t_base_type::TYPE_DOUBLE:
        out << "write_double(" << name << ");";
        break;
      default:
        throw "compiler error: no Crystal writer for base type " + t_base_type::t_base_name(tbase)
            + name;
      }
    } else if (type->is_enum()) {
      out << "write_i32(" << name << ".to_i32)";
    }
    out << endl;
  } else {
    std::cout << "DO NOT KNOW HOW TO SERIALIZE FIELD " << name.c_str() << " TYPE " << type_name(type).c_str() << "\n";
  }
}

void t_cr_generator::generate_serialize_struct(t_cr_ofstream& out, t_struct* tstruct, string prefix)
{
  out.indent() << prefix << ".write(oprot)" << endl;
}

void t_cr_generator::render_property_type(t_cr_ofstream& out, t_type* field_type,int key)
{

  out << "id: " << std::to_string(key);
  t_type* type = get_true_type(field_type);
  if (type->is_binary()) {
    out << ", binary: true";
  }
}

THRIFT_REGISTER_GENERATOR(
    cr,
    "Crystal",
    "    rubygems:        Add a \"require 'rubygems'\" line to the top of each generated file.\n"
    "    namespaced:      Generate files in idiomatic namespaced directories.\n")
