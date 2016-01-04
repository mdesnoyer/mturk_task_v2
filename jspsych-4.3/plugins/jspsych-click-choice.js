/**
 * Nick Dufour
 * December 2015
 *
 * This adapts an existing plugin (free-sort) to permit presentation of images
 * arranged into trials (K images per trial, N trials). The images are
 * presented for some amount of time, and the user is asked to select one
 * image (by clicking) according to the instructions, which are presented
 * first. After each click, the trial ends and the next one begins.
 * Additionally, the trial will auto-advance if too much time elapses.
 *
 *
 */

(function( $ ) {
	jsPsych["click-choice"] = (function(){

		var plugin = {};

		plugin.create = function(params) {
			var trials = new Array(params.stimuli.length);
			for(var i = 0; i < trials.length; i++)
			{
				trials[i] = {
					// the images to display, their coordinates, heights and
					// widths.
					"stimuli": params.stimuli[i],
					// how long the trial proceeds for (in msec)
					"trial_time": params.trial_time || -1,
					// whether or not a response causes the trial to advance
					"response_ends_trial" : (typeof params.response_ends_trial === 'undefined') ? true : params.response_ends_trial,
					// the prompt to display
					"prompt": (typeof params.prompt === 'undefined') ? '' : params.prompt,
					// where to display the prompt: "above" or "below"
                    "prompt_location": params.prompt_location || "above",
                    // how wide the canvas is
                    "choice_area_width": params.choice_area_width || 800,
                    // how high the canvas is
                    "choice_area_height": params.choice_area_height || 800,
                    // whether or not it's a keep or reject
                    "action_type": params.action_type || 'keep',
                    // how long to display the click feedback
                    "post_click_delay": params.post_click_delay || 0,
				};
				trials[i].type = "click-choice";
                // other information needed for the trial method can be added here

                // supporting the generic data object with the following line
                // is always a good idea. it allows people to pass in the data
                // parameter, but if they don't it gracefully adds an empty object
                // in it's place.
                trials[i].data = (typeof params.data === 'undefined') ? {} : params.data[i];
			}
			return trials;
		};

		plugin.trial = function(display_element, trial) {
            trial = jsPsych.pluginAPI.evaluateFunctionParameters(trial);
            var trial_ended = false;
			// store the response
			var response = {rt: -1, choice: -1}

			// this array holds handlers from setTimeout calls
			// that need to be cleared if the trial ends early
			var setTimeoutHandlers = [];

			// check if there is a prompt and if it is shown above
            if (trial.prompt && trial.prompt_location == "above") {
                display_element.append(trial.prompt);
            }

            display_element.append($('<div>', {
                "id": "jspsych-click-choice-arena",
                "class": "jspsych-click-choice-arena",
                "css": {
                    "position": "relative",
                    "width": trial.choice_area_width,
                    "height": trial.choice_area_height
                }
            }));

            // check if prompt exists and if it is shown below
            if (trial.prompt && trial.prompt_location == "below") {
                display_element.append(trial.prompt);
            }

            // display the images
            for (var i = 0; i < trial.stimuli.length; i++) {
                $("#jspsych-click-choice-arena").append($('<img>', {
                    "src": trial.stimuli[i].file,
                    "id": trial.stimuli[i].id,
                    "alt":i,
                    "class": "jspsych-click-choice-clickable",
                    "css": {
                        "position": "absolute",
                        "top": trial.stimuli[i].y,
                        "left": trial.stimuli[i].x,
                        "width": trial.stimuli[i].width,
                        "height": trial.stimuli[i].height
                    }
                }));
            }

            function after_response(choice, choice_idx) {
            	// measure the RT
            	var end_time = Date.now();
            	response.rt = end_time - start_time;
                response.choice_idx = choice_idx;
            	response.choice = choice;

            	// highlight the image
            	if (trial.action_type == 'keep'){
            		$("#" + choice).addClass("click-choice-responded-keep");
            	}
            	else {
            		$("#" + choice).addClass("click-choice-responded-reject");
            	}

            	console.log("Chose " + choice);

            	if (trial.response_ends_trial){
            		//console.log("Waiting for " + trial.post_click_delay);
            		setTimeout(function(){end_trial();},
            			trial.post_click_delay);
            		//end_trial();
            	}

            }

            function end_trial(){
            	// kill remaining setTimeout handlers
            	for (var i = 0; i < setTimeoutHandlers.length; i++){
            		clearTimeout(setTimeoutHandlers[i])
            	}

				// data saving
				var trial_data = {
				    action_type: trial.action_type,
				    trial_images: trial.stimuli,
				    choice: response.choice,
				    choice_idx: response.choice_idx,
				    stims: trial.stimuli,
				    rt: response.rt
				};

				jsPsych.data.write(trial_data)

				// clear the display
				display_element.html('')

				// move on to the next trial
				jsPsych.finishTrial();
            }

            $('.jspsych-click-choice-clickable').click(
            	function(event){
            		//var choice = $("#" + this.id);
            		if (trial_ended){
                        // this will help us avoid those irritating concurrency issues that I'm getting.
                        // for instance, if they click twice, or click after the trial is ended.
				        return;
				    } else {
				        trial_ended = true;
				    }
            		var choice = this.id;
                    var choice_idx = parseInt(this.alt);
            		// update the response
            		after_response(choice, choice_idx);

            	}
            	)


			// get the start time
			var start_time = Date.now();

			// end trial if time limit is set
			if (trial.trial_time > 0){
				var timer = setTimeout(function() {
				    console.log("Trial has timed out!")
				    if (trial_ended){
				        return;
				    } else {
				        trial_ended = true;
				    }
					end_trial();
				}, trial.trial_time);
				setTimeoutHandlers.push(timer);
			}

		};

		return plugin;
	})();
}) (jQuery);
