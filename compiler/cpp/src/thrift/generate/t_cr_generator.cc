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
  void generate_cr_union(t_cr_ofstream& out, t_struct* tstruct, bool is_exception);
  void generate_cr_union_validator(t_cr_ofstream& out, t_struct* tstruct);
  void generate_cr_function_helpers(t_function* tfunction);
  void generate_cr_simple_constructor(t_cr_ofstream& out, t_struct* tstruct);
  void generate_cr_simple_exception_constructor(t_cr_ofstream& out, t_struct* tstruct);
  void generate_cr_struct_decleration();
  void generate_serialize_struct(t_cr_ofstream& out, t_struct* tstruct, string prefix);
  void generate_serialize_union(t_cr_ofstream& out, t_struct* tunion);
  void generate_deserialize_union(t_cr_ofstream& out, t_struct* tunion);
  void generate_field_constants(t_cr_ofstream& out, t_struct* tstruct);
  void generate_field_constructors(t_cr_ofstream& out, t_struct* tstruct);
  void generate_field_defns(t_cr_ofstream& out, t_struct* tstruct);
  void generate_cr_struct_declaration(t_cr_ofstream& out, t_struct* tstruct, bool is_exception);
  void generate_field_data(t_cr_ofstream& out,
                           t_type* field_type,
                           const std::string& field_name,
                           t_field::e_req req);
  void generate_cr_thrift_write(t_cr_ofstream& out, t_struct* tstruct);
  void generate_cr_thrift_read(t_cr_ofstream& out, t_struct* tstruct);
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
                                  std::string class_name);

  void generate_deserialize_struct(t_cr_ofstream& out, t_struct* tstruct, std::string prefix = "");

  void generate_deserialize_container(t_cr_ofstream& out, t_type* ttype, std::string prefix = "");

  void generate_deserialize_set_element(t_cr_ofstream& out, t_set* tset, std::string prefix = "");

  void generate_deserialize_map_element(t_cr_ofstream& out, t_map* tmap, std::string prefix = "");

  void generate_deserialize_list_element(t_cr_ofstream& out,
                                         t_list* tlist,
                                         std::string prefix = "");

  void generate_serialize_field(t_cr_ofstream& out, t_field* tfield);

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

  std::string render_crystal_type(t_type* ttype,
                                     bool base = false,
                                     bool optional = false);
  void generate_struct_writer(t_cr_ofstream& out, t_struct* tstruct);
  std::string generate_cr_struct_required_fields(t_struct* tstruct);
  std::string cr_autogen_comment();
  std::string render_require_thrift();
  std::string render_includes();
  void render_property_type(t_cr_ofstream& out, t_field::e_req req, int key);
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

  std::string begin_namespace(std::vector<std::string>);
  std::string end_namespace(std::vector<std::string>);

private:
  /**
   * File streams
   */

  t_cr_ofstream f_types_;
  t_cr_ofstream f_consts_;
  t_cr_ofstream f_service_;

  std::string namespace_dir_;
  std::string require_prefix_;
  std::string module_begin_;
  std::string module_end_;

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

  module_begin_ = begin_namespace(crystal_modules(program_));
  // Print header
  f_types_ << cr_autogen_comment() << endl << render_require_thrift() << render_includes() << endl
           << module_begin_ << endl;

  f_consts_ << cr_autogen_comment() << endl
            << render_require_thrift() << "require \"./" << require_prefix_
            << underscore(program_name_) << "_types\"" << endl
            << endl
            << module_begin_ << endl;
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
  std::string end_module = end_namespace(crystal_modules(program_));
  f_types_ << end_module;
  f_consts_ << end_module;
  f_types_.close();
  f_consts_.close();
}

/**
 * Generates a typedef. This is not done in Crystal, types are all implicit.
 *
 * @param ttypedef The type definition
 */
void t_cr_generator::generate_typedef(t_typedef* ttypedef) {
  generate_rdoc(f_types_, ttypedef);
  indent(f_types_) << "alias " << ttypedef->get_symbolic() << " = " << render_crystal_type(ttypedef->get_type(), false, false) << endl << endl;
}

/**
 * Generates code for an enumerated type. Done using a class to scope
 * the values.
 *
 * @param tenum The enumeration
 */
void t_cr_generator::generate_enum(t_enum* tenum) {
  indent(f_types_) << "enum " << capitalize(tenum->get_name()) << endl;
  indent_up();

  vector<t_enum_value*> constants = tenum->get_constants();
  vector<t_enum_value*>::iterator c_iter;
  for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter) {
    int value = (*c_iter)->get_value();

    string name = capitalize(lowercase((*c_iter)->get_name()));

    generate_rdoc(f_types_, *c_iter);
    indent(f_types_) << name << " = " << value << endl;
  }
  indent_down();
  indent(f_types_) << "end" << endl << endl;
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
      if (type->is_binary()) {
        out << ".to_slice";
      }
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
    indent(out) << value->get_integer();
  } else if (type->is_struct() || type->is_xception()) {
    out << full_type_name(type) << ".new(" << endl;
    indent_up();
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
      indent(out);
      render_const_value(out, g_type_string, v_iter->first) << " => ";
      render_const_value(out, field_type, v_iter->second) << "," << endl;
    }
    indent_down();
    indent(out) << ")";
  } else if (type->is_map()) {
    bool first = true;
    t_type* ktype = ((t_map*)type)->get_key_type();
    t_type* vtype = ((t_map*)type)->get_val_type();
    out << render_crystal_type(type);
    if (value->get_map().empty())
    {
      out << ".new";
    } else {
      out << "{";
      const map<t_const_value*, t_const_value*, t_const_value::value_compare>& val = value->get_map();
      map<t_const_value*, t_const_value*, t_const_value::value_compare>::const_iterator v_iter;
      for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
        if (!first) {
          out << ", ";
        } else {
          first = false;
        }
        render_const_value(out, ktype, v_iter->first) << " => ";
        render_const_value(out, vtype, v_iter->second);
      }
      out << "}";
    }
  } else if (type->is_list() || type->is_set()) {
    bool first = true;
    t_type* etype;
    if (type->is_list()) {
      etype = ((t_list*)type)->get_elem_type();
    } else {
      etype = ((t_set*)type)->get_elem_type();
    }
    out << render_crystal_type(type);
    if (value->get_list().empty()) {
      out << ".new";
    } else {
      out << "{";
      const vector<t_const_value*>& val = value->get_list();
      vector<t_const_value*>::const_iterator v_iter;
      for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
        if (!first) {
          out << ", ";
        } else {
          first = false;
        }
        render_const_value(out, etype, *v_iter);
      }
      out << "}";
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
  } else if (tstruct->is_xception()) {
    generate_xception(tstruct);
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
  indent(out) << "class " << type_name(tstruct);
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

  auto& out = f_types_;
  indent(out) << "class " << type_name(txception) << " < Exception" << endl;
  indent_up();
  indent(out) << "include ::Thrift::Struct" << endl << endl;

  const std::vector<t_field*>& xception_fields = txception->get_members();
  std::vector<t_field*>::const_iterator f_iter;

  out << generate_cr_struct_required_fields(txception);

  generate_field_defns(out, txception);

  out << endl;
  generate_cr_struct_initializer(out, txception);
  out << endl;
  generate_cr_thrift_write(out, txception);
  out << endl;
  generate_cr_thrift_read(out, txception);

  indent_down();
  indent(out) << "end" << endl << endl;
}

/**
 * Generates a crystal struct
 */
void t_cr_generator::generate_cr_struct(t_cr_ofstream& out,
                                        t_struct* tstruct,
                                        bool is_exception = false,
                                        bool is_helper = false) {
  generate_rdoc(out, tstruct);
  indent(out) << "class " << type_name(tstruct) << endl;
  indent_up();
  indent(out) << "include ::Thrift::Struct" << endl << endl;

  const std::vector<t_field*>& fields = tstruct->get_members();
  std::vector<t_field*>::const_iterator f_iter;

  out << generate_cr_struct_required_fields(tstruct);

  generate_field_defns(out, tstruct);

  out << endl;
  generate_cr_struct_initializer(out, tstruct);
  out << endl;
  generate_cr_thrift_write(out, tstruct);
  out << endl;
  generate_cr_thrift_read(out, tstruct);

  indent_down();
  indent(out) << "end" << endl << endl;
}

void t_cr_generator::generate_cr_struct_initializer(t_cr_ofstream& out, t_struct* tstruct)
{
    // we take a copy because we need to sort these by requiredness
  std::vector<t_field*> members = tstruct->get_members();
  std::vector<t_field*>::iterator required_iter = std::partition(std::begin(members), std::end(members), [](const t_field* ele) {
    return ele->get_req() == t_field::T_REQUIRED;
  });
  std::vector<t_field*>::iterator default_value_iter = std::partition(required_iter, std::end(members), [](const t_field* ele) {
    return ele->get_value() != nullptr;
  });
  std::vector<t_field*>::const_iterator m_iter;
  bool first = true;
  indent(out) << "def initialize(";
  // required fields come first in initialization order
  for (m_iter = members.cbegin(); m_iter != required_iter; ++m_iter) {
    if (first) {
      first = false;
    } else {
      out << ", ";
    }
    out << "@" << (*m_iter)->get_name();
  }
  //non-required fields with defaults come after
  for (m_iter = required_iter; m_iter != default_value_iter; ++m_iter) {
    if (first) {
      first = false;
    } else {
      out << ", ";
    }
    out << "@" << (*m_iter)->get_name() << " = ";
    render_const_value(out, (*m_iter)->get_type(), (*m_iter)->get_value());
  }

  //non-required fields w/o defaults come after
  for (m_iter = default_value_iter; m_iter != std::cend(members); ++m_iter) {
      if (first) {
        first = false;
      } else {
        out << ", ";
      }
      out << "@" << (*m_iter)->get_name() << " = nil";
  }
  out << ")" << endl;
  indent_up();
  {
    for (m_iter = members.cbegin(); m_iter != required_iter; ++m_iter) {
      indent(out) << "@__required_fields__" << type_name(tstruct) << "_set[\"" << (*m_iter)->get_name() << "\"] = true" << endl;
    }
  }
  indent_down();
  indent(out) << "end" << endl;

}

/**
 * Generates a Crystal union
 */
void t_cr_generator::generate_cr_union(t_cr_ofstream& out,
                                       t_struct* tstruct,
                                       bool is_exception = false) {
  (void)is_exception;
  generate_rdoc(out, tstruct);
  indent(out) << "class " << type_name(tstruct) << endl;
  indent_up();
  indent(out) << "include ::Thrift::Union" << endl << endl;

  // generate_field_constructors(out, tstruct);

  generate_field_defns(out, tstruct);
  generate_serialize_union(out, tstruct);
  generate_cr_union_validator(out, tstruct);
  generate_deserialize_union(out, tstruct);

  indent_down();
  indent(out) << "end" << endl << endl;
}

void t_cr_generator::generate_field_constructors(t_cr_ofstream& out, t_struct* tstruct) {

  // indent(out) << "class << self" << endl;
  // indent_up();

  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;


  indent(out) << "def initialize(";
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
    // indent(out) << "  " << tstruct->get_name() << ".new(:" << field_name << ", val)" << endl;
    // indent(out) << "end" << endl;
  }
  out << ")" << endl;
  indent(out) << "end" << endl << endl;
  if (!first) {
    indent(out) << "def initialize; end" << endl;
  }
  // indent_down();
  // indent(out) << "end" << endl;

  out << endl;
}

std::string t_cr_generator::render_crystal_type(t_type* ttype,
                                                   bool base,
                                                   bool optional) {
  std::string rendered_type = "";
  ttype = get_true_type(ttype);
  if (ttype->is_base_type()) {
    t_base_type::t_base base = ((t_base_type*)ttype)->get_base();
    switch (base) {
    case t_base_type::TYPE_BOOL:
      rendered_type += "Bool";
      break;
    case t_base_type::TYPE_STRING:
      if (ttype->is_binary()) {
        rendered_type += "Bytes";
      } else {
        rendered_type += "String";
      }
      break;
    case t_base_type::TYPE_VOID:
      rendered_type += "Nil";
      break;
    case t_base_type::TYPE_UUID:
      rendered_type += "UUID";
      break;
    case t_base_type::TYPE_I8:
      rendered_type += "Int8";
      break;
    case t_base_type::TYPE_I16:
      rendered_type += "Int16";
      break;
    case t_base_type::TYPE_I32:
      rendered_type += "Int32";
      break;
    case t_base_type::TYPE_I64:
      rendered_type += "Int64";
      break;
    case t_base_type::TYPE_DOUBLE:
      rendered_type += "Float64";
      break;
    default:
      break;
    }
  } else if (ttype->is_enum()) {
    rendered_type += full_type_name(ttype);
  } else if (ttype->is_xception() || ttype->is_struct()) {
    rendered_type += full_type_name(ttype);
  } else if (ttype->is_map()) {
    rendered_type += "Hash";
    if (base) {
      return rendered_type;
    }
    rendered_type += "(" + render_crystal_type(((t_map*)ttype)->get_key_type()) + ", " + render_crystal_type(((t_map*)ttype)->get_val_type()) + ")";
  } else if (ttype->is_list() || ttype->is_set()) {
    t_type* ele_type;
    if (ttype->is_list()) {
      ele_type = ((t_list*)ttype)->get_elem_type();
      rendered_type += "Array";
      if (base) {
        return rendered_type;
      }
      rendered_type += "(" + render_crystal_type(ele_type, base, optional) + ")";
    } else {
      ele_type = ((t_set*)ttype)->get_elem_type();
      rendered_type += "Set";
      if (base) {
        return rendered_type;
      }
      rendered_type += "(" + render_crystal_type(ele_type, base, optional) + ")";
    }
  }
  if (optional) {
    rendered_type += "?";
  }

  return rendered_type;
}

void t_cr_generator::generate_field_defns(t_cr_ofstream& out, t_struct* tstruct) {
  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;

  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    generate_rdoc(out, tstruct);
    if (tstruct->is_union()) {
      indent(out) << "union_property ";
      generate_union_field_data(out, (*f_iter)->get_type(), (*f_iter)->get_name());
    } else {
      indent(out) << "struct_property ";
      generate_field_data(out, (*f_iter)->get_type(), (*f_iter)->get_name(),
                          (*f_iter)->get_req());
    }
    out << endl;
  }
  out << endl;
}

void t_cr_generator::generate_field_data(t_cr_ofstream& out,
                                         t_type* field_type,
                                         const std::string& field_name = "",
                                         t_field::e_req req = t_field::T_OPTIONAL) {
  // field_type = get_true_type(field_type);
  out << field_name << " : " << render_crystal_type(get_true_type(field_type), false, req != t_field::T_REQUIRED);
}

void t_cr_generator::generate_union_field_data(t_cr_ofstream& out,
                                               t_type* field_type,
                                               const std::string& field_name) {

  out << field_name << " : " << render_crystal_type(get_true_type(field_type));
}

std::string t_cr_generator::begin_namespace(vector<std::string> modules) {
  std::string scope = "";
  for (auto& module : modules) {
    scope += indent() + "module " + module + endl;
    indent_up();
  }
  return scope;
}

std::string t_cr_generator::end_namespace(vector<std::string> modules) {
  std::string scope = "";
  for (vector<std::string>::reverse_iterator m_iter = modules.rbegin(); m_iter != modules.rend();
       ++m_iter) {
    indent_down();
    scope += indent() + "end" + endl;
  }
  return scope;
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


  f_service_ << module_begin_ << endl;
  indent_up();
  indent(f_service_) << "module " << capitalize(tservice->get_name()) << endl;
  indent_up();

  // Generate the three main parts of the service
  generate_service_client(tservice);
  generate_service_server(tservice);
  generate_service_helpers(tservice);
  generate_service_skeleton(tservice);

  indent_down();
  indent(f_service_) << "end" << endl;

  std::string module_end = end_namespace(crystal_modules(tservice->get_program()));
  indent_down();
  f_service_ << module_end;

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

  indent(f_service_) << "# HELPER FUNCTIONS AND STRUCTURES" << endl << endl;

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

  indent(f_service_) << "class Client" << extends_client << endl;
  indent_up();

  indent(f_service_) << "include ::Thrift::Client" << endl << endl;

  // Generate client method implementations
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::const_iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    t_struct* arg_struct = (*f_iter)->get_arglist();
    const vector<t_field*>& fields = arg_struct->get_members();
    vector<t_field*>::const_iterator fld_iter;
    string funname = (*f_iter)->get_name();

    // Open function
    indent(f_service_) << "def " << function_signature(*f_iter) << endl;
    indent_up();
    indent(f_service_) << "send_" << funname << "(";

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
      indent(f_service_);
      if (!(*f_iter)->get_returntype()->is_void()) {
        f_service_ << "return ";
      }
      f_service_ << "recv_" << funname << "()" << endl;
    }
    indent_down();
    indent(f_service_) << "end" << endl;
    f_service_ << endl;

    indent(f_service_) << "def send_" << function_signature(*f_iter) << endl;
    indent_up();

    std::string argsname = capitalize((*f_iter)->get_name() + "_args");
    std::string messageSendProc = (*f_iter)->is_oneway() ? "send_oneway_message" : "send_message";

    indent(f_service_) << messageSendProc << "(\"" << funname << "\", " << argsname;

    for (fld_iter = fields.begin(); fld_iter != fields.end(); ++fld_iter) {
      f_service_ << ", " << (*fld_iter)->get_name() << ": " << (*fld_iter)->get_name();
    }

    f_service_ << ")" << endl;

    indent_down();
    indent(f_service_) << "end" << endl;

    if (!(*f_iter)->is_oneway()) {
      std::string resultname = capitalize((*f_iter)->get_name() + "_result");
      t_struct noargs(program_);

      t_function recv_function((*f_iter)->get_returntype(), string("recv_") + (*f_iter)->get_name(),
                               &noargs);
      // Open function
      f_service_ << endl;
      indent(f_service_) << "def " << function_signature(&recv_function) << endl;
      indent_up();

      indent(f_service_) << "fname, mtype, rseqid = receive_message_begin()" << endl;
      indent(f_service_) << "handle_exception(mtype)" << endl;

      indent(f_service_) << "if reply_seqid(rseqid)==false" << endl;
      indent(f_service_) << "  raise \"seqid reply faild\"" << endl;
      indent(f_service_) << "end" << endl;

      indent(f_service_) << "result = receive_message(" << resultname << ")" << endl;

      // Careful, only return _result if not a void function
      if (!(*f_iter)->get_returntype()->is_void()) {
        indent(f_service_) << "return result.success unless result.success.nil?" << endl;
      }

      t_struct* xs = (*f_iter)->get_xceptions();
      const std::vector<t_field*>& xceptions = xs->get_members();
      vector<t_field*>::const_iterator x_iter;
      for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
        indent(f_service_) << "raise result." << (*x_iter)->get_name() << " unless result."
                            << (*x_iter)->get_name() << ".nil?" << endl;
      }

      // Careful, only return _result if not a void function
      if ((*f_iter)->get_returntype()->is_void()) {
        indent(f_service_) << "return" << endl;
      } else {
        indent(f_service_) << "raise "
                               "::Thrift::ApplicationException.new(::Thrift::ApplicationException::"
                               "MISSING_RESULT, \""
                            << (*f_iter)->get_name() << " failed: unknown result\")" << endl;
      }

      // Close function
      indent_down();
      indent(f_service_) << "end" << endl << endl;
    }
  }

  indent_down();
  indent(f_service_) << "end" << endl << endl;
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
  indent(f_service_) << "class Processor(T)" << endl;
  indent_up();
  indent(f_service_) << "include ::Thrift::Processor" << extends_processor << endl;
  indent(f_service_) << "@handler : T" << endl << endl;

  // indent(f_service_) << "include ::Thrift::Processor" << endl << endl;

  // Generate the process subfunctions
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    generate_process_function(tservice, *f_iter);
  }

  indent_down();
  indent(f_service_) << "end" << endl << endl;
}

/**
 * Generates a process function definition.
 *
 * @param tfunction The function to write a dispatcher for
 */
void t_cr_generator::generate_process_function(t_service* tservice, t_function* tfunction) {
  (void)tservice;
  // Open function
  indent(f_service_) << "def process_" << decapitalize(tfunction->get_name()) << "(seqid, iprot, oprot)" << endl;
  indent_up();

  string argsname = capitalize(tfunction->get_name()) + "_args";
  string resultname = capitalize(tfunction->get_name()) + "_result";

  indent(f_service_) << "args = read_args(iprot, " << argsname << ")" << endl;

  t_struct* xs = tfunction->get_xceptions();
  const std::vector<t_field*>& xceptions = xs->get_members();
  vector<t_field*>::const_iterator x_iter;

  // Declare result for non oneway function
  if (!tfunction->is_oneway()) {
    indent(f_service_) << "result = " << resultname << ".new" << endl;
  }

  // Try block for a function with exceptions
  if (xceptions.size() > 0) {
    indent(f_service_) << "begin" << endl;
    indent_up();
  }

  // Generate the function call
  t_struct* arg_struct = tfunction->get_arglist();
  const std::vector<t_field*>& fields = arg_struct->get_members();
  vector<t_field*>::const_iterator f_iter;

  indent(f_service_);
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
    indent_down();
    for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
      indent(f_service_) << "rescue " << (*x_iter)->get_name() << " : "
                          << full_type_name((*x_iter)->get_type()) << endl;
      if (!tfunction->is_oneway()) {
        indent_up();
        indent(f_service_) << "result." << (*x_iter)->get_name() << " = " << (*x_iter)->get_name()
                            << endl;
        indent_down();
      }
    }
    indent(f_service_) << "end" << endl;
  }

  // Shortcut out here for oneway functions
  if (tfunction->is_oneway()) {
    indent(f_service_) << "return" << endl;
    indent_down();
    indent(f_service_) << "end" << endl << endl;
    return;
  }

  indent(f_service_) << "write_result(result, oprot, \"" << tfunction->get_name() << "\", seqid)"
                      << endl;

  // Close function
  indent_down();
  indent(f_service_) << "end" << endl << endl;
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
 * Renders the require of thrift itself
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
      return "::Thrift::Types::String";
    case t_base_type::TYPE_BOOL:
      return "::Thrift::Types::Bool";
    case t_base_type::TYPE_I8:
      return "::Thrift::Types::Byte";
    case t_base_type::TYPE_I16:
      return "::Thrift::Types::I16";
    case t_base_type::TYPE_I32:
      return "::Thrift::Types::I32";
    case t_base_type::TYPE_I64:
      return "::Thrift::Types::I64";
    case t_base_type::TYPE_DOUBLE:
      return "::Thrift::Types::Double";
    case t_base_type::TYPE_UUID:
      return "::Thrift::Types::Uuid";
    default:
      throw "compiler error: unhandled type";
    }
  } else if (type->is_enum()) {
    return "::Thrift::Types::I32";
  } else if (type->is_struct() || type->is_xception()) {
    return "::Thrift::Types::Struct";
  } else if (type->is_map()) {
    return "::Thrift::Types::Map";
  } else if (type->is_set()) {
    return "::Thrift::Types::Set";
  } else if (type->is_list()) {
    return "::Thrift::Types::List";
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
    generate_docstring_comment(out, "", "# ", tdoc->get_doc(), "");
  }
}

void t_cr_generator::generate_cr_union_validator(t_cr_ofstream& out, t_struct* tstruct) {
  indent(out) << "def validate" << endl;
  indent_up();

  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;

  indent(out) << "raise(StandardError, \"Union fields are not set.\") unless union_set?"
               << endl;

  // if field is an enum, check that its value is valid
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    const t_field* field = (*f_iter);

    if (field->get_type()->is_enum()) {
      indent(out) << "if get_set_field == :" << field->get_name() << endl;
      indent(out) << "  raise "
                      "::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, "
                      "\"Invalid value of field "
                   << field->get_name() << "!\") unless " << full_type_name(field->get_type())
                   << "::VALID_VALUES.include?(get_value)" << endl;
      indent(out) << "end" << endl;
    }
  }

  indent_down();
  indent(out) << "end" << endl << endl;
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
            //  << "require \"./" << get_include_prefix(*get_program()) << svcname << ".h\"" << endl
             << "require \"thrift\"" << endl
             << "include Thrift" << endl;

  f_skeleton << "class " << svcname << "Handler" << endl;
  indent_up();
  f_skeleton << indent() << "def initialize" << endl
             << "  # Your initialization goes here" << endl << indent() << "end" << endl << endl;

  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    // generate_java_doc(f_skeleton, *f_iter);
    f_skeleton << indent() << "def " << function_signature(*f_iter, "") << endl << indent()
               << "  # Your implementation goes here" << endl << indent() << " \""
               << (*f_iter)->get_name() << "\"" << endl << indent() << "end" << endl << endl;
  }

  indent_down();
  f_skeleton << "end" << endl << endl;

  f_skeleton << indent() << "def some_skeleton_main" << endl;
  indent_up();
  f_skeleton
      << indent() << "port = 9090" << endl << indent()
      << "handler = " << svcname << "Handler.new" << endl << indent()
      << "processor = " << svcname << "::Processor.new(handler)" << endl
      << indent() << "serverTransport = ServerSocketTransport.new(port)"
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

void t_cr_generator::generate_serialize_field(t_cr_ofstream& out, t_field* tfield)
{
  const std::string name = tfield->get_name();

  indent(out) << "@" << name << ".try do |" << name << "|" << endl;
  indent_up();
  {
    indent(out) << "oprot.write_field_begin(\"" << name << "\")" << endl
                << indent() << name << ".write to: oprot" << endl
                << indent() << "oprot.write_field_end" << endl;
  }
  indent_down();
  indent(out) << "end" << endl;
}

void t_cr_generator::generate_serialize_struct(t_cr_ofstream& out, t_struct* tstruct, string prefix)
{
  indent(out) << prefix << ".write(oprot)" << endl;
}

void t_cr_generator::render_property_type(t_cr_ofstream& out, t_field::e_req req,int key)
{

  out << "id: " << std::to_string(key) << ", req_in: " << std::boolalpha << (req == t_field::T_REQUIRED) << ", req_out: " << (req == t_field::T_OPT_IN_REQ_OUT || req == t_field::T_REQUIRED);
}

void t_cr_generator::generate_cr_thrift_write(t_cr_ofstream& out, t_struct* tstruct)
{

  std::vector<t_field*> fields = tstruct->get_members();
  std::vector<t_field*>::iterator required_iter = std::partition(std::begin(fields), std::end(fields), [](const t_field* field) {
    return field->get_req() == t_field::T_REQUIRED;
  });
  std::vector<t_field*>::iterator opt_in_req_out_iter = std::partition(required_iter, std::end(fields), [](const t_field* ele) {
    return ele->get_req() == t_field::T_OPT_IN_REQ_OUT;
  });

  indent(out) << "def write(to oprot : ::Thrift::BaseProtocol)" << endl;
  indent_up();
  {
    bool first = true;
    if (required_iter != std::begin(fields)) {
      indent(out) << "{% begin %}" << endl;
      indent(out) << "if !(%empty_fields = @__required_fields__" << type_name(tstruct) << "_set";
      if (opt_in_req_out_iter != required_iter) {
        bool first_opt = true;
        out << ".merge({";
        for (std::vector<t_field*>::const_iterator f_iter = opt_in_req_out_iter; f_iter != std::cend(fields); ++f_iter) {
          if (first_opt) {
            first_opt = false;
          } else {
            out << ", ";
          }
          out << "\"" << (*f_iter)->get_name() << "\" => !@" << (*f_iter)->get_name() << ".nil?";
        }
        out << "})";
      }
      out << ".select{|k,v| !v}).empty?" << endl;
      indent_up();
      {
        indent(out) << "raise ::Thrift::ProtocolException.new ::Thrift::ProtocolException::INVALID_DATA, \"Required write fields are unset: #{%empty_fields.keys}\"" << endl;
      }
      indent_down();
      indent(out) << "end" << endl;
      indent(out) << "{% end %}" << endl;
    }
    indent(out) << "oprot.write_recursion do" << endl;
    indent_up();
    {
      indent(out) << "oprot.write_struct_begin(self.class.name)" << endl << endl;
      for(std::vector<t_field*>::const_iterator f_iter = fields.cbegin(); f_iter != fields.cend(); ++f_iter) {
        generate_serialize_field(out, (*f_iter));
      }
      indent(out) << "oprot.write_field_stop" << endl;
      indent(out) << "oprot.write_struct_end" << endl;
    }
    indent_down();
    indent(out) << "end" << endl;
  }
  indent_down();
  indent(out) << "end" << endl;
}

void t_cr_generator::generate_cr_thrift_read(t_cr_ofstream& out, t_struct* tstruct)
{
  const std::vector<t_field*>& fields = tstruct->get_members();
  indent(out) << "def read(from iprot : ::Thrift::BaseProtocol)" << endl;
  indent_up();
  {
    indent(out) << "iprot.read_recursion do" << endl;
    indent_up();
    {
      indent(out) << "iprot.read_struct_begin" << endl;
      indent(out) << "loop do" << endl;
      indent_up();
      {
        indent(out) << "name, ftype, fid = iprot.read_field_begin" << endl;
        indent(out) << "next if ftype == ::Thrift::Types::Void" << endl;
        indent(out) << "break if ftype == ::Thrift::Types::Stop" << endl;
        indent(out) << "case {fid, ftype}" << endl;
        for(std::vector<t_field*>::const_iterator f_iter = fields.cbegin(); f_iter != fields.cend(); ++f_iter) {
          generate_deserialize_field(out, *f_iter, type_name(tstruct));
        }
        indent(out) << "else" << endl;
        indent_up();
        {
          indent(out) << "iprot.skip type" << endl;
        }
        indent_down();
        indent(out) << "end" << endl;
        indent(out) << "iprot.read_field_end" << endl;
      }
      indent_down();
      indent(out) << "end" << endl;
      indent(out) << "iprot.read_struct_end" << endl;
      if (std::count_if(std::cbegin(fields), std::cend(fields), [](const t_field* tfield) {return tfield->get_req() == t_field::T_REQUIRED;})) {
        indent(out) << "{% begin %}" << endl;
        indent(out) << "%empty_fields = @__required_fields__" << type_name(tstruct) << "_set.select{|k,v| !v}" << endl;
        indent(out) << "raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::INVALID_DATA, \"Required fields are not set: #{%empty_fields.keys}\") unless %empty_fields.empty?" << endl;
        indent(out) << "{% end %}" << endl;
      }
    }
    indent_down();
    indent(out) << "end" << endl;
  }
  indent_down();
  indent(out) << "end" << endl;
}

std::string t_cr_generator::generate_cr_struct_required_fields(t_struct* tstruct)
{
  std::string ret = "";
  std::vector<t_field*> fields = tstruct->get_members();
  std::vector<t_field*>::iterator required_iter = std::partition(std::begin(fields), std::end(fields), [](const t_field* ele) {
    return ele->get_req() == t_field::T_REQUIRED;
  });

  if (required_iter != std::begin(fields)) {
    ret += indent() + "@__required_fields__" + type_name(tstruct) + "_set = ";
    bool first = true;
    ret += "{";
    for (std::vector<t_field*>::const_iterator f_iter = std::cbegin(fields); f_iter != required_iter; ++f_iter) {
      if (!first) {
        ret += ", ";
      } else {
        first = false;
      }
      ret += "\"" + (*f_iter)->get_name() + "\" => false";
    }
    ret += "}" + endl;
  }
  return ret;
}

void t_cr_generator::generate_serialize_union(t_cr_ofstream& out, t_struct* tunion)
{
  const std::vector<t_field*>& fields = tunion->get_members();
  std::vector<t_field*>::const_iterator f_iter;

  indent(out) << "def write(to oprot : ::Thrift::BaseProtocol)" << endl;
  indent_up();
  {
    indent(out) << "oprot.write_recursion do" << endl;
    indent_up();
    {
      indent(out) << "oprot.write_struct_begin(self.class.name)" << endl;
      indent(out) << "case union_internal" << endl;
      for(f_iter = std::cbegin(fields); f_iter != std::cend(fields); ++f_iter) {
        indent(out) << "when .is_a?(" << render_crystal_type((*f_iter)->get_type());
        out << ")" << endl;
        indent_up();
        {
          indent(out) << "oprot.write_field_begin(\"" << (*f_iter)->get_name() << "\", " << render_crystal_type((*f_iter)->get_type()) << "::THRIFT_TYPE, " << (*f_iter)->get_key() << "_i16)" << endl;
          indent(out) << "@" << (*f_iter)->get_name() << ".write to: oprot" << endl;
          indent(out) << "oprot.write_field_end" << endl;
        }
        indent_down();
      }
      indent(out) << "end" << endl;
      indent(out) << "oprot.write_field_stop" << endl;
      indent(out) << "oprot.write_struct_end" << endl;
    }
    indent_down();
    indent(out) << "end" << endl;
  }
  indent_down();
  indent(out) << "end" << endl;
}

void t_cr_generator::generate_deserialize_union(t_cr_ofstream& out, t_struct* tunion)
{
  const std::vector<t_field*>& fields = tunion->get_members();
  std::vector<t_field*>::const_iterator f_iter;

  indent(out) << "def self.read(from iprot : ::Thrift::BaseProtocol)" << endl;
  indent_up();
  {
    indent(out) << "iprot.read_recursion do" << endl;
    indent_up();
    {
      indent(out) << "recieved_union = " << type_name(tunion) << ".allocate" << endl;
      indent(out) << "iprot.read_struct_begin" << endl;
      indent(out) << "loop do" << endl;
      indent_up();
      {
        indent(out) << "fname, ftype, fid = iprot.read_field_begin" << endl;
        indent(out) << "break if ftype == ::Thrift::Types::Stop" << endl;
        indent(out) << "raise \"Too Many fields for Union\" if union_set?" << endl;
        indent(out) << "case {fid,ftype}" << endl;
        for (f_iter = std::cbegin(fields); f_iter != std::cend(fields); ++f_iter) {
          indent(out) << "when {" << (*f_iter)->get_key() << ", " << render_crystal_type((*f_iter)->get_type(), true) << "::THRIFT_TYPE}" << endl;
          indent_up();
          {
              indent(out) << "received_union." << (*f_iter)->get_name() << " = " << render_crystal_type((*f_iter)->get_type()) << ".read from: iprot" << endl;
          }
          indent_down();
        }
        indent(out) << "else" << endl;
        indent_up();
        {
          indent(out) << "iprot.skip type" << endl;
        }
        indent_down();
        indent(out) << "end" << endl;

        indent(out) << "iprot.read_field_end" << endl;
      }
      indent_down();
      indent(out) << "end" << endl;
      indent(out) << "iprot.read_struct_end" << endl;
      indent(out) << "received_union.validate" << endl;
      indent(out) << "received_union" << endl;
    }
    indent_down();
    indent(out) << "end" << endl;
  }
  indent_down();
  indent(out) << "end" << endl;
}

  void t_cr_generator::generate_deserialize_field(t_cr_ofstream& out, t_field* tfield, std::string class_name) {

  indent(out) << "when {" << tfield->get_key() << ", " << render_crystal_type(tfield->get_type(), true) << "::THRIFT_TYPE}" << endl;
  indent_up();
  {
    indent(out) << "@" << tfield->get_name() << " = " << render_crystal_type(tfield->get_type()) << ".read from: oprot" << endl;
    if(tfield->get_req() == t_field::T_REQUIRED) {
      indent(out) << "@__required_fields__" << class_name << "_set[\"" << tfield->get_name() << "\"] = true" << endl;
    }
  }
  indent_down();
}


THRIFT_REGISTER_GENERATOR(
    cr,
    "Crystal",
    "    rubygems:        Add a \"require 'rubygems'\" line to the top of each generated file.\n"
    "    namespaced:      Generate files in idiomatic namespaced directories.\n")
