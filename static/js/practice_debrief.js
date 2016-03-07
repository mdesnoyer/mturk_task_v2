var passed_practice = true;

function LogGamma(Z) {
	with (Math) {
		var S=1+76.18009173/Z-86.50532033/(Z+1)+24.01409822/(Z+2)-1.231739516/(Z+3)+.00120858003/(Z+4)-.00000536382/(Z+5);
		var LG= (Z-.5)*log(Z+4.5)-(Z+4.5)+log(S*2.50662827465);
	}
	return LG
}

function Betinc(X,A,B) {
	var A0=0;
	var B0=1;
	var A1=1;
	var B1=1;
	var M9=0;
	var A2=0;
	var C9;
	while (Math.abs((A1-A2)/A1)>.00001) {
		A2=A1;
		C9=-(A+M9)*(A+B+M9)*X/(A+2*M9)/(A+2*M9+1);
		A0=A1+C9*A0;
		B0=B1+C9*B0;
		M9=M9+1;
		C9=M9*(B-M9)*X/(A+2*M9-1)/(A+2*M9);
		A1=A0+C9*A1;
		B1=B0+C9*B1;
		A0=A0/B1;
		B0=B0/B1;
		A1=A1/B1;
		B1=1;
	}
	return A1/A
}

function compute_binprob(X, N, P) {
    with (Math) {
		if (N<=0) {
			alert("sample size must be positive must be positive")
		} else if ((P<0)||(P>1)) {
			alert("probability must be between 0 and 1")
		} else if (X<0) {
			bincdf=0
		} else if (X>=N) {
			bincdf=1
		} else {
			X=floor(X);
			Z=P;
			A=X+1;
			B=N-X;
			S=A+B;
			BT=exp(LogGamma(S)-LogGamma(B)-LogGamma(A)+A*log(Z)+B*log(1-Z));
			if (Z<(A+1)/(S+2)) {
				Betacdf=BT*Betinc(Z,A,B)
			} else {
				Betacdf=1-BT*Betinc(1-Z,B,A)
			}
			bincdf=1-Betacdf;
		}
		bincdf=round(bincdf*100000)/100000;
	}
    return bincdf;
}

// Actually, it appears more appropriate to calculate the p-value from the Chi-Square scores.
//
// NOTE: this only works when all trials have the _same_ number of items!


    /*  The following JavaScript functions for calculating normal and
        chi-square probabilities and critical values were adapted by
        John Walker from C implementations
        written by Gary Perlman of Wang Institute, Tyngsboro, MA
        01879.  Both the original C code and this JavaScript edition
        are in the public domain.  */

    /*  POZ  --  probability of normal z value

        Adapted from a polynomial approximation in:
                Ibbetson D, Algorithm 209
                Collected Algorithms of the CACM 1963 p. 616
        Note:
                This routine has six digit accuracy, so it is only useful for absolute
                z values < 6.  For z values >= to 6.0, poz() returns 0.0.
    */

    function poz(z) {
        var y, x, w;
        var Z_MAX = 6.0;              /* Maximum meaningful z value */

        if (z == 0.0) {
            x = 0.0;
        } else {
            y = 0.5 * Math.abs(z);
            if (y >= (Z_MAX * 0.5)) {
                x = 1.0;
            } else if (y < 1.0) {
                w = y * y;
                x = ((((((((0.000124818987 * w
                         - 0.001075204047) * w + 0.005198775019) * w
                         - 0.019198292004) * w + 0.059054035642) * w
                         - 0.151968751364) * w + 0.319152932694) * w
                         - 0.531923007300) * w + 0.797884560593) * y * 2.0;
            } else {
                y -= 2.0;
                x = (((((((((((((-0.000045255659 * y
                               + 0.000152529290) * y - 0.000019538132) * y
                               - 0.000676904986) * y + 0.001390604284) * y
                               - 0.000794620820) * y - 0.002034254874) * y
                               + 0.006549791214) * y - 0.010557625006) * y
                               + 0.011630447319) * y - 0.009279453341) * y
                               + 0.005353579108) * y - 0.002141268741) * y
                               + 0.000535310849) * y + 0.999936657524;
            }
        }
        return z > 0.0 ? ((x + 1.0) * 0.5) : ((1.0 - x) * 0.5);
    }


    var BIGX = 20.0;                  /* max value to represent exp(x) */

    function ex(x) {
        return (x < -BIGX) ? 0.0 : Math.exp(x);
    }

    /*  POCHISQ  --  probability of chi-square value

              Adapted from:
                      Hill, I. D. and Pike, M. C.  Algorithm 299
                      Collected Algorithms for the CACM 1967 p. 243
              Updated for rounding errors based on remark in
                      ACM TOMS June 1985, page 185
    */

    function pochisq(x, df) {
        var a, y, s;
        var e, c, z;
        var even;                     /* True if df is an even number */

        var LOG_SQRT_PI = 0.5723649429247000870717135; /* log(sqrt(pi)) */
        var I_SQRT_PI = 0.5641895835477562869480795;   /* 1 / sqrt(pi) */

        if (x <= 0.0 || df < 1) {
            return 1.0;
        }

        a = 0.5 * x;
        even = !(df & 1);
        if (df > 1) {
            y = ex(-a);
        }
        s = (even ? y : (2.0 * poz(-Math.sqrt(x))));
        if (df > 2) {
            x = 0.5 * (df - 1.0);
            z = (even ? 1.0 : 0.5);
            if (a > BIGX) {
                e = (even ? 0.0 : LOG_SQRT_PI);
                c = Math.log(a);
                while (z <= x) {
                    e = Math.log(z) + e;
                    s += ex(c * z - a - e);
                    z += 1.0;
                }
                return s;
            } else {
                e = (even ? 1.0 : (I_SQRT_PI / Math.sqrt(a)));
                c = 0.0;
                while (z <= x) {
                    e = e * (a / z);
                    c = c + e;
                    z += 1.0;
                }
                return c * y + s;
            }
        } else {
            return s;
        }
    }

function get_chi_square(counts, total, n){
    // calculates the chi-square score given a table of counts, the total number of observations, and the number of
    // items presented each trial.
    with (Math) {
        var chiSquare = 0;
        var exp_val = (1.0 / n) * total; // the expected value
        for (var key = 0; key < n; key++){
            if (counts.hasOwnProperty(key)){
                chiSquare += Math.pow((counts[key] - exp_val), 2) / exp_val;
            } else {
                chiSquare += Math.pow(-exp_val, 2) / exp_val;
            }
        }
    }
    return chiSquare;
}

function comp_chi_pval(counts, total, n){
    var chiSquare = get_chi_square(counts, total, n);
    var dof = n - 1;
    return pochisq(chiSquare, dof);

}

function get_bias_string(thresh){
    /*
    * Computes the bias of the experiment and returns a string
    * and returns a string to this effect.
    */
    var start_time = (new Date()).getTime();

    var bias = {};

    // compute the relative count frequencies.
    var trials = jsPsych.data.getTrialsOfType("click-choice");
    var cmax = 0;

    for (var i = 0; i < trials.length; i++){
        if (trials[i].trial_images.length > cmax){
            cmax = trials[i].trial_images.length}
        if (trials[i].choice < 0){
            continue;
        }
        var choice = trials[i].choice_idx;
        if (choice in bias){
            bias[choice] += 1;
        } else {
            bias[choice] = 1;
        }
    }

    var total = 0;

    for (var key in bias) {
        if (bias.hasOwnProperty(key)){
            total += bias[key];
        }
    }

    var rstring = "Because the images are randomly arranged, you should click in any given position about " + (100.0 / cmax).toFixed(0) + "% of the time. ";
    rstring += "Some workers try to cheat the system by clicking in one place repeatedly without actually looking at the images. ";
    rstring += "To avoid this, we measure the positions in which people click to make sure they are not clicking in the same place over and over just to get ";
    rstring += "through the experiment as quickly as possible. Let's see how you did.</br></br>"

    for (var i = 0; i < cmax; i++){
        var choice_num = i + 1;
        if (bias.hasOwnProperty(i)){
            rstring += "You clicked on the image in position " + choice_num + " <strong>" + ((bias[i] * 100.0)/total).toFixed(0) + "%</strong> of the time</br>";
        } else {
            rstring += "You clicked on the image in position " + choice_num + " <strong>0%</strong> of the time</br>";
        }
    }
    rstring += '</br>';
    // compute the probability
    var prob = 1 - comp_chi_pval(bias, total, cmax);
    if(prob > thresh){
        rstring += '<font color="red">Your probability is too high! Please repeat the practice.</font>';
        passed_practice = false;
    } else {
        rstring += '<font color="green">Your probability is within limits for behaving randomly.</font>';
    }
    return rstring;
  };

function get_contradictions_string(thresh, attribute){
    var keep_tuple_index = {};
    var reject_tuple_index = {};
    var trials = jsPsych.data.getTrialsOfType("click-choice");
    var valid_trials_count = 0;
    var tot_conts = 0;
    var exemplar_trial = 0;
    var exemplar_trial_idx = 0;
    for (var i = 0; i < trials.length; i++){
        if (trials[i].choice < 0){
            continue;
        } else {
            valid_trials_count++;
        }
        if (trials[i].action_type == 'keep'){
            keep_tuple_index[trials[i].global_tup_idx] = trials[i].image_idx_map[trials[i].choice_idx];
        } else {
            reject_tuple_index[trials[i].global_tup_idx] = trials[i].image_idx_map[trials[i].choice_idx];
        }
    }
    for (var key in keep_tuple_index) {
        if(key in reject_tuple_index){
            if (keep_tuple_index[key] == reject_tuple_index[key]){
                exemplar_trial_idx = key;
                tot_conts++;
            }
        }
    }
    /* fetch the actual exemplar idx */
    for (var i = 0; i < trials.length; i++){
        if (trials[i].global_tup_idx == exemplar_trial_idx){
            exemplar_trial = i;
            break;
        }
    }
    var mean_conts = tot_conts * 2.0 / valid_trials_count;  // since only keep trials are counted
    var rstring = "The last measure, but possibly the most important, is the number of times a worker contradicts ";
    rstring += "themselves. If they pick the same image as both the most " + attribute + " and the least " + attribute;
    rstring += " then it is clear that they are not following directions.</br></br>"
    if (tot_conts == 0){
        rstring += '<font color="green">However, you never contradicted yourself! Congratulations.</font>';
        return rstring
    }
    rstring += 'For instance, when asked to choose among these images: </br></br><center>';
    for (var i = 0; i < trials[exemplar_trial].stims.length; i++){
        rstring += '<img src="' + trials[exemplar_trial].stims[i].file + '" style="width:' + trials[exemplar_trial].stims[i].width + 'px;height:' + trials[exemplar_trial].stims[i].height +'px;">';
    }
    rstring += '</center></br></br>you chose</br></br>';
    rstring += '<center><img src="' + trials[exemplar_trial].stims[trials[exemplar_trial].choice_idx].file + '" style="width:' + trials[exemplar_trial].stims[trials[exemplar_trial].choice_idx].width + 'px;height:' + trials[exemplar_trial].stims[trials[exemplar_trial].choice_idx].height +'px;"></center>';
    rstring += '</br></br>as both the most ' + attribute + ' and the least ' + attribute + '. ';
    rstring += 'In total, <strong>' + Math.floor(mean_conts * 100.0) + '%</strong> of choices were contradicted. <br><br>';
    if (mean_conts > thresh){
        rstring += '<font color="red">Unfortunately you made too many contradictions! Please repeat the practice.</font>';
        passed_practice = false;
    } else {
        rstring += '<font color="green">You are within limits for contradictions.</font>';
    }
    return rstring;
}

function get_missed_string(thresh){
    /*
    * Returns a string to inform the worker if the missed too many trials.
    */
    with(Math){
        var start_time = (new Date()).getTime();
        var trials = jsPsych.data.getTrialsOfType("click-choice");
        var missed_trials = 0;
        for (var i = 0; i < trials.length; i++) {
            if (trials[i].choice < 0){
                missed_trials++;
            }
        }
        var mean_missed_trials = missed_trials * 1.0 / trials.length;

        var rstring = "If a worker misses too many trials, in other words fails to respond before the trials ends, then ";
        rstring += "their data cannot be used.</br></br>";
        rstring += "You missed <strong>" + Math.floor(mean_missed_trials * 100.0) + "%</strong> of trials.</br></br>";
        if (mean_missed_trials > thresh){
            rstring += '<font color="red">You missed too many trials! Please repeat the practice.</font>';
            passed_practice = false;
        } else {
            rstring += '<font color="green">You are within limits for missed trials.</font>';
        }
        return rstring;
    }
}

function get_rt_string(thresh, min_speed){
    /*
    * Determines if the response time is too fast.
    */
    with(Math){
        var start_time = (new Date()).getTime();
        var trials = jsPsych.data.getTrialsOfType("click-choice");
        var sum_rt = 0;
        var num_below = 0;
        var valid_trials_count = 0;
        for (var i = 0; i < trials.length; i++) {
            if (trials[i].choice < 0){
                continue;
            }
            sum_rt += trials[i].rt;
            valid_trials_count++;
            if (trials[i].rt <= min_speed){
                num_below++;
            }
        }
        var mean_rt = Math.floor(sum_rt / valid_trials_count);
        var mean_below = Math.floor(num_below / valid_trials_count);
    }
    var rstring = "Some workers try to speed through the experiment by clicking as fast as possible. To avoid this, we ";
    rstring += "measure your mean reaction time; in other words, how quickly you make decisions. If a worker goes ";
    rstring += "faster than a human can reasonably make decisions, we exclude their data.</br></br>";
    rstring += "Let's see how you did.</br></br>";
    rstring += "Your average reaction time was: <strong>" + mean_rt + "</strong> milliseconds.</br></br>";
    rstring += "<strong>" + mean_below * 100 + "%</strong> of your trials are too fast.</br></br>";
    if (mean_below > thresh){
        rstring += '<font color="red">Too many of your trials went too fast!</font>';
        passed_practice = false;
    } else {
        rstring += '<font color="green">You are within limits for reaction time.</font>';
    }
    return rstring;
}