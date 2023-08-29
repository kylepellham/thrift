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
    end
  end
end