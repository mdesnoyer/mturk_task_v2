/**
* Nick Dufour
* December 2015
*
* Displays the bias of a user--i.e., how often they click in one position or the other.
*
* NOT IMPLEMENTED
**/
(function( $ ) {
    jsPsych["display-bias"] = (function(){
        var plugin = {};

        var bias = {};

        plugin.create = function(params){};

        plugin.trial = function(display_element){
            var start_time = (new Date()).getTime();

            // compute the relative count frequencies.
            var trials = jsPsych.getTrialsOfType("click-choice");

            for (var i = 0; i < trials.length; i++){
                var choice = trials[i].choice_idx;
                if (choice in bias){
                    bias[choice] += 1;
                } else {
                    bias[choice] = 1;
                }
            }

            var total = 0;

            for (var key in p) {
                if (p.hasOwnProperty(key)){
                    total += p[key];
                }
            }


        }
    })();
})(JQuery);