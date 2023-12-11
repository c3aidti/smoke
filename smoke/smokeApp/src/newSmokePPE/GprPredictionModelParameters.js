function getSerializedKernel(){
    var typeName = this.type().typeName();
    var thisType = TypeRef.make({"typeName": typeName}).toType();
    var thisObj = thisType.fetch({"filter":Filter.eq('id',this.id),"include":"serializedKernel"}).objs[0];
    return thisObj.serializedKernel;
}