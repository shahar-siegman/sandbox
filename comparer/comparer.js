  
module.exports =
    {
        defaultCompare:
        function defaultCompare(A, B) {
            return A < B ? -1 :
                A == B ? 0 : 1;
        },
  
        objectComparison:
        function objectComparison(fieldsA, fieldsB) {
            fieldsB = fieldsB || fieldsA;
            var l1 = fieldsA.length, l2 = fieldsB.length;
            var l = Math.min(l1, l2);
            return function (objA, objB) {
                if (!fieldsA || !fieldsB) return 0; // if no fields to compare, all objects are equal
                for (var i = 0; objA[fieldsA[i]] == objB[fieldsB[i]] && i < l; i++);
                if (i == l) return 0;
  
                return objA[fieldsA[i]] < objB[fieldsB[i]] || objA[fieldsA[i]] === null ? -1 : 1
            }
        },
  
        objectComparison2:
        function objectComparison(fieldsA, fieldsB) {
            fieldsB = fieldsB || fieldsA;
            var l1 = fieldsA.length, l2 = fieldsB.length;
            var l = Math.min(l1, l2);
            return function (objA, objB) {
                if (!fieldsA || !fieldsB) return 0; // if no fields to compare, all objects are equal
                for (var i = 0; compareAnyTypes(objA[fieldsA[i]], objB[fieldsB[i]]) == 0 && i < l; i++);
                if (i == l) return 0;
  
                return compareAnyTypes(objA[fieldsA[i]], objB[fieldsB[i]])
            }
        }
    }
  
compareSimple = (x, y) => x < y ? -1 : (x == y ? 0 : 1)
  
function compareAnyTypes(objA, objB) {
    if (objA === null || objA === undefined || objA !== objA)
        return (objB === null || objB === undefined || objB !== objB ) ? 0 : -1
    if (objB === null || objB === undefined || objB !== objB)
        return 1
  
    if ((assertType(objA, Number) || assertType(objA, String)) && (assertType(objB, Number) || assertType(objB, String))) {
        if (typeof objA != typeof objB) { // when trying to compare number to string, check if the comparison is consistent, if not, compare strings
            var a = compareSimple(objA, objB)
            if (a == - compareSimple(objB, objA))
                return a
            objA = '' + objA
            objB = '' + objB
        }
        return compareSimple(objA, objB)
    }
  
    if (assertType(objA, Date) && assertType(objB, Date)) { // dates are compared through their getTime()
        return compareSimple(objA.getTime(), objB.getTime())
    }
}
  
  
function assertType(obj, type) { // generously contributed by Arash Milani on https://arashmilani.com/post?id=35
    return obj.constructor.name === type.name
}