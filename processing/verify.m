clear;
T = readtable("/home/local/ASUAD/tkhandai/RA_work/archcomphelper/archcompwebsite/uploads/c3110c7c-7928-486c-b432-4f92d19f39ca_BO_ARCH2022_08112022.csv",...
    'Format', "auto", "Delimiter", ',');

categories = convertvars(T, ["system", "property", "instance"], "categorical");

[groups, group_names] = findgroups(categories(:,1:3));

unique_groups = unique(groups);
dic = containers.Map;

for i = 1:length(unique_groups)
    subTable = categories(groups == i, :);
    
    dic(strjoin(string(group_names{i,:}), "___")) = subTable;
end

dickeys = dic.keys();

for i = 1:length(dickeys)
    x = dic(dickeys{i});
    for j = 1:size(x,1)
        system = x{j,"system"};
        y = eval(x{j, "input"}{1,1});
        switch system
            case "AT"
                addpath(genpath("FALS/transmission"))
                init_transmission;
                u_ = y(:,1:end);
                t_ = y(:,1);
                u = [t_,u_];
                T = max(t_);
                [t_out, y_out] = run_transmission([], u_, T);
            case "CC"
                addpath(genpath("FALS/chasing-cars"))
                init_cars;
                u_ = y(:,1:end);
                t_ = y(:,1);
                u = [t_,u_];
                T = max(t_);
                [t_out, y_out] = run_cars(u_,T);
            case "NN"
                addpath(genpath("FALS/neural"))
                init_neural;
                u_ = y(:,1:end);
                t_ = y(:,1);
                u = [t_,u_];
                T = max(t_);
                [t_out, y_out] = run_neural(u_,T);
            case "AFC_normal"
                addpath(genpath("FALS/powertrain"))
                init_powertrain;
                u_ = y(:,1:end);
                t_ = y(:,1);
                u = [t_,u_];
                T = max(t_);
                [t_out, y_out] = run_powertrain(u_,T);
            case "AFC_power"
                addpath(genpath("FALS/powertrain"))
                u_ = y(:,1:end);
                t_ = y(:,1);
                u = [t_,u_];
                T = max(t_);
                [t_out, y_out] = run_powertrain(u_,T);
        end
    end
end