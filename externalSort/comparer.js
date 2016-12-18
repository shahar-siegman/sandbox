
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
                return objA[fieldsA[i]] < objB[fieldsB[i]] ? -1 : 1
            }
        }
    }