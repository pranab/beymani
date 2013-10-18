#!/usr/bin/ruby

require '../lib/util.rb'      


custCount = ARGV[0].to_i

custIDs = []
amountDist = CategoricalField.new("L",35,"M",53,"H",12)
typeDist =  CategoricalField.new("N",85,"H",15)
timeElapsedDist = CategoricalField.new("L",35,"N",45,"S",20)


idGen = IdGenerator.new
1.upto custCount do
	custIDs << idGen.generate(10)
end

#num of transactions
1.upto 15 do
	#number of customers
	1.upto custCount do
		if (rand(10) < 9)
			cid = custIDs[rand(custIDs.length)]
			xid = idGen.generate(12)
			puts "#{cid},#{xid},#{amountDist.value}#{typeDist.value}#{timeElapsedDist.value}"
		end
	end
end
