require "./thrift_union.cr"

module Thrift
  module Struct
    macro included
      def each_field
        {%
          vars = @type.instance_vars + @type.methods.select(&.annotation(UnionVar))
        %}
        {% for var in vars %}
          {% if var.is_a?(MetaVar) %}
            \{{var.id}} = yield \{{var.id}}, \{{var.stringify}}, \{{var.type}}
          {% else %}
            \{{var.id}} = yield \{{var.id}}, \{{var.stringify}}, \{{var.return_type}}
          {% end %}
        {% end %}
      end

      def set_field(field, &)
        case field
        {% for var in @type.methods.select {|method| @type.instance_vars.includes?(method.stringify) || method.annotation(MetaVar)} %}
        when {{var.upcase.id}}
          self.{{var.id}} = yield self.{{var.id}}
        {% end %}
        else
          raise "Not a field"
        end
      end
    end
  end
end