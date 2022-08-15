/**
* Copyright (c) 2022, C3 AI DTI, Development Operations Team
* All rights reserved. License: https://github.com/c3aidti/.github
**/
/**
 * Callback that is called synchronously during a request 
 * that creates objs after those objs are created. 
 *
 * @param objs
 *  List of objs that were created.  
 *  The objs will already have been created.  
 *  By default, only the id is present in the objs. 
 *  If more fields are desired a dependency annotation can  be specified 
 * (e.g. `@dependency(include = "field1, field2...")`. 
 * Then the objs will have at least those requested fields.
 * @return List of any errors that were encountered.
 */