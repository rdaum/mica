/** Copyright (C) Ryan Daum 2001, 2002, 2003.  See COPYING for details.
*/
#ifndef METAOBJECTS_HH
#define METAOBJECTS_HH

/** Defines a global list of pointers to objects which are "meta-delegates"
 *  for the each of the builtin types.  OptSlots defined on these objects
 *  are bound via delegation to builtin-types.
 */
namespace mica {
  
  class Var;

  struct MetaObjects
  {
    /** Parent of all types
     */
    static Var TypeMeta;

    /** Meta-prototypes for specific types -- dispatching gets
     *  redirected to these Objects by the classes' get/assign/slots
     *  methods.  Methods on them are filled in by native methods.
     */
    static Var AtomMeta;
    static Var SequenceMeta;
    static Var ListMeta;
    static Var StringMeta;
    static Var MapMeta;
    static Var SetMeta;
    static Var SymbolMeta;
    static Var ErrorMeta;


    /** Built-in global objects
     */
    static Var SystemMeta;    // the $System utility object, holds 
                              // methods for administration of a mica system.

    static Var Lobby;         // The Lobby is the top of the name hierarchy.

    static Var AnyMeta;       // Used for wildcard verb matching


    /** Functions
     */
    static void initialize( const Var &lobby ); // Initialize all metaobject references
    static void cleanup();    // Clear all the references to NONE  

    static var_vector delegates_for( Type::Identifier type_id );

  };
}

#endif
